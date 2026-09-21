import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/otp/actor
import gleeunit
import gleeunit/should
import rx
import rx/flow
import rx/future
import rx/runtime

pub type Request {
  Request(id: String, tenant: String, sequence: Int)
}

pub type GroupResult {
  GroupResult(tenant: String, sequence: Int)
}

pub type IngressEvent {
  QueueMessage(String)
  SocketMessage(String)
}

pub type CanonicalEvent {
  UserChanged(String)
  InvoicePaid(String)
}

type ControlledWork {
  ControlledWork(Int, fn(Result(Int, String)) -> Nil)
}

type DedupeMessage {
  FirstSeen(String, fn(Bool) -> Nil)
  StopDedupe
}

type GroupMessage {
  Submit(Request, fn(Result(GroupResult, String)) -> Nil)
  StopGroups
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn use_case_01_async_fifo_with_async_processing_test() {
  let emitters = process.new_subject()
  let work = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.create(fn(emitter) {
      process.send(emitters, emitter)
      fn() { Nil }
    })
  let pipeline =
    source
    |> flow.concat_map(fn(value) {
      future.new(fn(resolve) {
        process.send(work, ControlledWork(value, resolve))
        fn() { Nil }
      })
    })
  let assert Ok(subscription) =
    rx.subscribe(
      pipeline,
      runtime_,
      rx.observer(
        fn(value) { process.send(outputs, value) },
        fn(_) { Nil },
        fn() { Nil },
      ),
    )
  let assert Ok(emitter) = process.receive(from: emitters, within: 1000)

  rx.next(emitter, 1)
  rx.next(emitter, 2)
  let assert Ok(ControlledWork(1, resolve1)) =
    process.receive(from: work, within: 1000)
  process.receive(from: work, within: 20) |> should.equal(Error(Nil))

  resolve1(Ok(10))
  let assert Ok(ControlledWork(2, resolve2)) =
    process.receive(from: work, within: 1000)
  process.receive(from: outputs, within: 1000) |> should.equal(Ok(10))

  resolve2(Ok(20))
  process.receive(from: outputs, within: 1000) |> should.equal(Ok(20))

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn use_case_02_deduplicate_requests_in_memory_test() {
  let dedupe = start_dedupe()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let requests: rx.Observable(Request, String) =
    rx.from_list([
      Request(id: "a", tenant: "t1", sequence: 1),
      Request(id: "a", tenant: "t1", sequence: 2),
      Request(id: "b", tenant: "t1", sequence: 3),
    ])
  let unique =
    requests
    |> flow.filter_async_concurrent(
      fn(request) {
        let Request(id:, tenant: _, sequence: _) = request
        first_seen(dedupe, id)
      },
      2,
    )
  let assert Ok(subscription) =
    rx.subscribe(
      unique,
      runtime_,
      rx.observer(
        fn(request) { process.send(outputs, request) },
        fn(_) { Nil },
        fn() { Nil },
      ),
    )

  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(Request(id: "a", tenant: "t1", sequence: 1)))
  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(Request(id: "b", tenant: "t1", sequence: 3)))
  process.receive(from: outputs, within: 20) |> should.equal(Error(Nil))

  rx.unsubscribe(subscription)
  process.send(dedupe, StopDedupe)
  runtime.stop(runtime_)
}

pub fn use_case_03_group_requests_by_key_test() {
  let groups = start_groups()
  let outputs = process.new_subject()
  let errors = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let requests: rx.Observable(Request, String) =
    rx.from_list([
      Request(id: "a1", tenant: "a", sequence: 1),
      Request(id: "b1", tenant: "b", sequence: 1),
      Request(id: "a2", tenant: "a", sequence: 2),
      Request(id: "b2", tenant: "b", sequence: 2),
    ])
  let grouped =
    requests
    |> flow.merge_map(fn(request) { group_submit(groups, request) }, 4)
  let assert Ok(subscription) =
    rx.subscribe(
      grouped,
      runtime_,
      rx.observer(
        fn(result) { process.send(outputs, result) },
        fn(reason) { process.send(errors, reason) },
        fn() { Nil },
      ),
    )

  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(GroupResult(tenant: "a", sequence: 1)))
  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(GroupResult(tenant: "b", sequence: 1)))
  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(GroupResult(tenant: "a", sequence: 2)))
  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(GroupResult(tenant: "b", sequence: 2)))
  process.receive(from: errors, within: 20) |> should.equal(Error(Nil))

  rx.unsubscribe(subscription)
  process.send(groups, StopGroups)
  runtime.stop(runtime_)
}

pub fn use_case_04_merge_server_push_sources_at_ingress_test() {
  let emitters = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let merged: rx.Observable(IngressEvent, String) =
    rx.create(fn(emitter) {
      process.send(emitters, emitter)
      fn() { Nil }
    })
  let assert Ok(subscription) =
    rx.subscribe(
      merged,
      runtime_,
      rx.observer(
        fn(event) { process.send(outputs, event) },
        fn(_) { Nil },
        fn() { Nil },
      ),
    )
  let assert Ok(emitter) = process.receive(from: emitters, within: 1000)

  rx.next(emitter, QueueMessage("job-1"))
  rx.next(emitter, SocketMessage("peer-1"))

  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(QueueMessage("job-1")))
  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(SocketMessage("peer-1")))

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn use_case_05_rebase_heterogeneous_inputs_test() {
  let emitters = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let canonical: rx.Observable(CanonicalEvent, String) =
    rx.create(fn(emitter) {
      process.send(emitters, emitter)
      fn() { Nil }
    })
  let assert Ok(subscription) =
    rx.subscribe(
      canonical,
      runtime_,
      rx.observer(
        fn(event) { process.send(outputs, event) },
        fn(_) { Nil },
        fn() { Nil },
      ),
    )
  let assert Ok(emitter) = process.receive(from: emitters, within: 1000)

  rx.next(emitter, UserChanged("user-7"))
  rx.next(emitter, InvoicePaid("invoice-9"))

  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(UserChanged("user-7")))
  process.receive(from: outputs, within: 1000)
  |> should.equal(Ok(InvoicePaid("invoice-9")))

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

fn start_dedupe() -> process.Subject(DedupeMessage) {
  let assert Ok(started) =
    actor.new(dict.new())
    |> actor.on_message(handle_dedupe)
    |> actor.start
  let actor.Started(pid: _, data: subject) = started
  subject
}

fn handle_dedupe(
  state: Dict(String, Nil),
  message: DedupeMessage,
) -> actor.Next(Dict(String, Nil), DedupeMessage) {
  case message {
    StopDedupe -> actor.stop()
    FirstSeen(id, resolve) ->
      case dict.get(state, id) {
        Ok(_) -> {
          resolve(False)
          actor.continue(state)
        }
        Error(_) -> {
          resolve(True)
          actor.continue(dict.insert(state, id, Nil))
        }
      }
  }
}

fn first_seen(
  dedupe: process.Subject(DedupeMessage),
  id: String,
) -> future.Future(Bool, String) {
  future.new(fn(resolve) {
    process.send(dedupe, FirstSeen(id, fn(value) { resolve(Ok(value)) }))
    fn() { Nil }
  })
}

fn start_groups() -> process.Subject(GroupMessage) {
  let assert Ok(started) =
    actor.new(dict.new())
    |> actor.on_message(handle_groups)
    |> actor.start
  let actor.Started(pid: _, data: subject) = started
  subject
}

fn handle_groups(
  state: Dict(String, Int),
  message: GroupMessage,
) -> actor.Next(Dict(String, Int), GroupMessage) {
  case message {
    StopGroups -> actor.stop()
    Submit(request, resolve) -> {
      let Request(id: _, tenant:, sequence:) = request
      let expected = case dict.get(state, tenant) {
        Ok(last) -> last + 1
        Error(_) -> 1
      }
      case sequence == expected {
        True -> {
          resolve(Ok(GroupResult(tenant:, sequence:)))
          actor.continue(dict.insert(state, tenant, sequence))
        }
        False -> {
          resolve(Error("out-of-order group request"))
          actor.continue(state)
        }
      }
    }
  }
}

fn group_submit(
  groups: process.Subject(GroupMessage),
  request: Request,
) -> future.Future(GroupResult, String) {
  future.new(fn(resolve) {
    process.send(groups, Submit(request, resolve))
    fn() { Nil }
  })
}

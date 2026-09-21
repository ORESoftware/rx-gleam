import gleam/erlang/process
import gleeunit
import gleeunit/should
import rx
import rx/protocol
import rx/runtime

pub type Event {
  Value(Int)
  Failed(String)
  Completed
  TornDown
  Transformed(Int)
  ProtocolViolation(protocol.ProtocolError)
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn synchronous_terminal_source_runs_late_teardown_once_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let source: rx.Observable(Int, String) =
    rx.create(fn(emitter) {
      rx.next(emitter, 1)
      rx.complete(emitter)
      fn() { process.send(events, TornDown) }
    })

  let assert Ok(subscription) = rx.subscribe(source, runtime_, observer(events))

  receive(events) |> should.equal(Value(1))
  receive(events) |> should.equal(Completed)
  receive(events) |> should.equal(TornDown)

  // Cancellation after terminal cleanup is idempotent.
  rx.unsubscribe(subscription)
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))

  runtime.stop(runtime_)
}

pub fn late_emission_after_unsubscribe_is_ignored_test() {
  let ready = process.new_subject()
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let source: rx.Observable(Int, String) =
    rx.create(fn(emitter) {
      process.send(ready, emitter)
      fn() { process.send(events, TornDown) }
    })

  let assert Ok(subscription) = rx.subscribe(source, runtime_, observer(events))
  let assert Ok(emitter) = process.receive(from: ready, within: 1000)

  rx.unsubscribe(subscription)
  receive(events) |> should.equal(TornDown)

  rx.next(emitter, 99)
  rx.complete(emitter)
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))

  runtime.stop(runtime_)
}

pub fn operators_never_run_after_terminal_error_test() {
  let events = process.new_subject()
  let diagnostics = process.new_subject()
  let assert Ok(runtime_) =
    runtime.start_checked(fn(diagnostic) {
      case diagnostic {
        runtime.ProtocolViolation(reason) ->
          process.send(diagnostics, ProtocolViolation(reason))
        runtime.DuplicateTeardownRegistration -> Nil
      }
    })

  let source: rx.Observable(Int, String) =
    rx.create(fn(emitter) {
      rx.next(emitter, 1)
      rx.error(emitter, "boom")
      // Deliberately invalid producer behavior. The runtime must reject this
      // before the downstream map callback can observe the value.
      rx.next(emitter, 2)
      fn() { process.send(events, TornDown) }
    })

  let mapped =
    source
    |> rx.map(fn(value) {
      process.send(events, Transformed(value))
      value * 10
    })

  let assert Ok(_subscription) =
    rx.subscribe(mapped, runtime_, observer(events))

  receive(events) |> should.equal(Transformed(1))
  receive(events) |> should.equal(Value(10))
  receive(events) |> should.equal(Failed("boom"))
  receive(diagnostics)
  |> should.equal(ProtocolViolation(protocol.NotificationAfterTermination))
  receive(events) |> should.equal(TornDown)
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))

  runtime.stop(runtime_)
}

pub fn multiple_subscriptions_are_isolated_on_one_runtime_test() {
  let left = process.new_subject()
  let right = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) = rx.from_list([1, 2, 3])

  let assert Ok(_left_subscription) =
    rx.subscribe(source, runtime_, observer(left))
  let assert Ok(_right_subscription) =
    rx.subscribe(
      source |> rx.map(fn(value) { value * 10 }),
      runtime_,
      observer(right),
    )

  receive(left) |> should.equal(Value(1))
  receive(left) |> should.equal(Value(2))
  receive(left) |> should.equal(Value(3))
  receive(left) |> should.equal(Completed)

  receive(right) |> should.equal(Value(10))
  receive(right) |> should.equal(Value(20))
  receive(right) |> should.equal(Value(30))
  receive(right) |> should.equal(Completed)

  runtime.stop(runtime_)
}

fn observer(events: process.Subject(Event)) -> rx.Observer(Int, String) {
  rx.observer(
    fn(value) { process.send(events, Value(value)) },
    fn(reason) { process.send(events, Failed(reason)) },
    fn() { process.send(events, Completed) },
  )
}

fn receive(events: process.Subject(Event)) -> Event {
  let assert Ok(event) = process.receive(from: events, within: 1000)
  event
}

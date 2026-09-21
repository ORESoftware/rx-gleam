import gleam/erlang/process
import gleeunit
import gleeunit/should
import rx
import rx/flow
import rx/future
import rx/runtime

pub type SourceCommand {
  Emit(Int)
  Finish
  FailSource(String)
}

pub type WorkerEvent {
  WorkerStarted(Int, process.Subject(Result(Int, String)))
}

pub type OutputEvent {
  Value(Int)
  Failed(String)
  Completed
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn concat_map_accepts_async_input_while_work_is_running_test() {
  let ready = process.new_subject()
  let workers = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let source = controlled_async_source(ready)
  let mapped =
    flow.concat_map(source, fn(value) { controlled_future(value, workers) })

  let assert Ok(subscription) =
    rx.subscribe(mapped, runtime_, output_observer(outputs))
  let assert Ok(commands) = process.receive(from: ready, within: 1000)

  process.send(commands, Emit(1))
  let assert Ok(WorkerStarted(1, gate1)) =
    process.receive(from: workers, within: 1000)

  // These values arrive later, while item 1 is still asynchronously running.
  process.send(commands, Emit(2))
  process.send(commands, Emit(3))
  process.send(commands, Finish)

  // concat_map has exactly one active projected Future.
  let assert Error(Nil) = process.receive(from: workers, within: 20)

  process.send(gate1, Ok(10))
  let assert Ok(WorkerStarted(2, gate2)) =
    process.receive(from: workers, within: 1000)
  receive_output(outputs) |> should.equal(Value(10))

  process.send(gate2, Ok(20))
  let assert Ok(WorkerStarted(3, gate3)) =
    process.receive(from: workers, within: 1000)
  receive_output(outputs) |> should.equal(Value(20))

  process.send(gate3, Ok(30))
  receive_output(outputs) |> should.equal(Value(30))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn merge_map_bounds_concurrency_and_emits_completion_order_test() {
  let ready = process.new_subject()
  let workers = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let mapped =
    controlled_async_source(ready)
    |> flow.merge_map(fn(value) { controlled_future(value, workers) }, 2)

  let assert Ok(subscription) =
    rx.subscribe(mapped, runtime_, output_observer(outputs))
  let assert Ok(commands) = process.receive(from: ready, within: 1000)

  process.send(commands, Emit(1))
  process.send(commands, Emit(2))
  process.send(commands, Emit(3))
  process.send(commands, Finish)

  let assert Ok(WorkerStarted(1, gate1)) =
    process.receive(from: workers, within: 1000)
  let assert Ok(WorkerStarted(2, gate2)) =
    process.receive(from: workers, within: 1000)
  let assert Error(Nil) = process.receive(from: workers, within: 20)

  // Completing #2 frees one slot, so queued #3 begins before #1 is done.
  process.send(gate2, Ok(20))
  let assert Ok(WorkerStarted(3, gate3)) =
    process.receive(from: workers, within: 1000)
  receive_output(outputs) |> should.equal(Value(20))

  process.send(gate1, Ok(10))
  receive_output(outputs) |> should.equal(Value(10))

  process.send(gate3, Ok(30))
  receive_output(outputs) |> should.equal(Value(30))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn ordered_async_map_processes_concurrently_but_emits_fifo_test() {
  let ready = process.new_subject()
  let workers = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let mapped =
    controlled_async_source(ready)
    |> flow.map_ordered(fn(value) { controlled_future(value, workers) }, 2)

  let assert Ok(subscription) =
    rx.subscribe(mapped, runtime_, output_observer(outputs))
  let assert Ok(commands) = process.receive(from: ready, within: 1000)

  process.send(commands, Emit(1))
  process.send(commands, Emit(2))
  process.send(commands, Finish)

  let assert Ok(WorkerStarted(1, gate1)) =
    process.receive(from: workers, within: 1000)
  let assert Ok(WorkerStarted(2, gate2)) =
    process.receive(from: workers, within: 1000)

  // #2 finishes first but cannot pass #1 in InputOrder mode.
  process.send(gate2, Ok(20))
  let assert Error(Nil) = process.receive(from: outputs, within: 20)

  process.send(gate1, Ok(10))
  receive_output(outputs) |> should.equal(Value(10))
  receive_output(outputs) |> should.equal(Value(20))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn projected_error_is_fail_fast_and_late_success_is_ignored_test() {
  let ready = process.new_subject()
  let workers = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let mapped =
    controlled_async_source(ready)
    |> flow.merge_map(fn(value) { controlled_future(value, workers) }, 2)

  let assert Ok(subscription) =
    rx.subscribe(mapped, runtime_, output_observer(outputs))
  let assert Ok(commands) = process.receive(from: ready, within: 1000)

  process.send(commands, Emit(1))
  process.send(commands, Emit(2))
  process.send(commands, Emit(3))

  let assert Ok(WorkerStarted(1, gate1)) =
    process.receive(from: workers, within: 1000)
  let assert Ok(WorkerStarted(2, gate2)) =
    process.receive(from: workers, within: 1000)

  process.send(gate2, Error("boom"))
  receive_output(outputs) |> should.equal(Failed("boom"))

  // Failure cancels worker #1 and drops queued #3. Sending to gate1 after its
  // unlinked worker has been cancelled is harmless and must not emit anything.
  process.send(gate1, Ok(10))
  let assert Error(Nil) = process.receive(from: outputs, within: 20)
  let assert Error(Nil) = process.receive(from: workers, within: 20)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

fn controlled_async_source(
  ready: process.Subject(process.Subject(SourceCommand)),
) -> rx.Observable(Int, String) {
  rx.create(fn(emitter) {
    let pid =
      process.spawn_unlinked(fn() {
        let commands = process.new_subject()
        process.send(ready, commands)
        source_loop(commands, emitter)
      })

    fn() { process.kill(pid) }
  })
}

fn source_loop(
  commands: process.Subject(SourceCommand),
  emitter: rx.Emitter(Int, String),
) -> Nil {
  case process.receive(from: commands, within: 5000) {
    Error(Nil) -> rx.error(emitter, "source command timeout")
    Ok(Emit(value)) -> {
      rx.next(emitter, value)
      source_loop(commands, emitter)
    }
    Ok(Finish) -> rx.complete(emitter)
    Ok(FailSource(reason)) -> rx.error(emitter, reason)
  }
}

fn controlled_future(
  value: Int,
  workers: process.Subject(WorkerEvent),
) -> future.Future(Int, String) {
  future.new(fn(resolve) {
    let pid =
      process.spawn_unlinked(fn() {
        let gate = process.new_subject()
        process.send(workers, WorkerStarted(value, gate))

        case process.receive(from: gate, within: 5000) {
          Ok(result) -> resolve(result)
          Error(Nil) -> resolve(Error("worker gate timeout"))
        }
      })

    fn() { process.kill(pid) }
  })
}

fn output_observer(
  outputs: process.Subject(OutputEvent),
) -> rx.Observer(Int, String) {
  rx.observer(
    fn(value) { process.send(outputs, Value(value)) },
    fn(reason) { process.send(outputs, Failed(reason)) },
    fn() { process.send(outputs, Completed) },
  )
}

fn receive_output(outputs: process.Subject(OutputEvent)) -> OutputEvent {
  let assert Ok(event) = process.receive(from: outputs, within: 1000)
  event
}

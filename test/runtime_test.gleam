import gleam/erlang/process
import gleeunit
import gleeunit/should
import rx
import rx/protocol
import rx/runtime

pub type Event {
  Value(Int)
  Completed
  ProtocolViolation(protocol.ProtocolError)
  DuplicateTeardown
  TornDown
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn runtime_rejects_post_terminal_notifications_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) =
    runtime.start_checked(fn(diagnostic) {
      case diagnostic {
        runtime.ProtocolViolation(reason) ->
          process.send(events, ProtocolViolation(reason))
        runtime.DuplicateTeardownRegistration ->
          process.send(events, DuplicateTeardown)
      }
    })

  let source: rx.Observable(Int, String) =
    rx.create(fn(emitter) {
      rx.next(emitter, 1)
      rx.complete(emitter)
      rx.next(emitter, 2)
      fn() { process.send(events, TornDown) }
    })

  let assert Ok(_subscription) =
    rx.subscribe(
      source,
      runtime_,
      rx.observer(
        fn(value) { process.send(events, Value(value)) },
        fn(_) { Nil },
        fn() { process.send(events, Completed) },
      ),
    )

  process.receive(from: events, within: 1_000)
  |> should.equal(Ok(Value(1)))
  process.receive(from: events, within: 1_000)
  |> should.equal(Ok(Completed))
  process.receive(from: events, within: 1_000)
  |> should.equal(Ok(ProtocolViolation(protocol.NotificationAfterTermination)))
  process.receive(from: events, within: 1_000)
  |> should.equal(Ok(TornDown))

  runtime.stop(runtime_)
}

pub fn cancellation_is_idempotent_and_teardown_runs_once_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let source: rx.Observable(Int, String) =
    rx.create(fn(_) {
      fn() { process.send(events, TornDown) }
    })

  let assert Ok(subscription) =
    rx.subscribe(
      source,
      runtime_,
      rx.observer(fn(_) { Nil }, fn(_) { Nil }, fn() { Nil }),
    )

  rx.unsubscribe(subscription)
  rx.unsubscribe(subscription)

  process.receive(from: events, within: 1_000)
  |> should.equal(Ok(TornDown))
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))

  runtime.stop(runtime_)
}

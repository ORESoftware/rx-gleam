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
  NestedSubscribeFailed
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

  process.receive(from: events, within: 1000)
  |> should.equal(Ok(Value(1)))
  process.receive(from: events, within: 1000)
  |> should.equal(Ok(Completed))
  process.receive(from: events, within: 1000)
  |> should.equal(Ok(ProtocolViolation(protocol.NotificationAfterTermination)))
  process.receive(from: events, within: 1000)
  |> should.equal(Ok(TornDown))

  runtime.stop(runtime_)
}

pub fn cancellation_is_idempotent_and_teardown_runs_once_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let source: rx.Observable(Int, String) =
    rx.create(fn(_) { fn() { process.send(events, TornDown) } })

  let assert Ok(subscription) =
    rx.subscribe(
      source,
      runtime_,
      rx.observer(fn(_) { Nil }, fn(_) { Nil }, fn() { Nil }),
    )

  rx.unsubscribe(subscription)
  rx.unsubscribe(subscription)

  process.receive(from: events, within: 1000)
  |> should.equal(Ok(TornDown))
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))

  runtime.stop(runtime_)
}

pub fn runtime_stop_runs_active_subscription_teardown_once_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.create(fn(_) { fn() { process.send(events, TornDown) } })

  let assert Ok(_subscription) =
    rx.subscribe(
      source,
      runtime_,
      rx.observer(fn(_) { Nil }, fn(_) { Nil }, fn() { Nil }),
    )

  runtime.stop(runtime_)

  process.receive(from: events, within: 1000)
  |> should.equal(Ok(TornDown))
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))
}

pub fn observer_can_subscribe_reentrantly_on_same_runtime_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let outer: rx.Observable(Int, String) = rx.of(1)

  let assert Ok(_outer_subscription) =
    rx.subscribe(
      outer,
      runtime_,
      rx.observer(
        fn(value) {
          let inner: rx.Observable(Int, String) = rx.of(value + 1)
          case
            rx.subscribe(
              inner,
              runtime_,
              rx.observer(
                fn(inner_value) { process.send(events, Value(inner_value)) },
                fn(_) { Nil },
                fn() { Nil },
              ),
            )
          {
            Ok(_) -> Nil
            Error(_) -> process.send(events, NestedSubscribeFailed)
          }
        },
        fn(_) { Nil },
        fn() { Nil },
      ),
    )

  process.receive(from: events, within: 1000)
  |> should.equal(Ok(Value(2)))
  process.receive(from: events, within: 20)
  |> should.equal(Error(Nil))

  runtime.stop(runtime_)
}

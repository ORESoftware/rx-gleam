import gleam/erlang/process
import gleeunit
import gleeunit/should
import rx
import rx/eager
import rx/runtime

pub type Event {
  Value(Int)
  Failed(String)
  Completed
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn eager_pipeline_is_immediate_and_composable_test() {
  let sequence: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3, 4, 5])
    |> eager.map(fn(value) { value * 3 })
    |> eager.filter(fn(value) { value > 6 })
    |> eager.take(2)

  sequence
  |> eager.to_result
  |> should.equal(Ok([9, 12]))
}

pub fn eager_scan_flat_map_and_fold_test() {
  let sequence: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3])
    |> eager.flat_map(fn(value) { eager.from_list([value, value * 10]) })
    |> eager.scan(0, fn(total, value) { total + value })

  sequence
  |> eager.to_result
  |> should.equal(Ok([1, 11, 13, 33, 36, 66]))

  sequence
  |> eager.fold(0, fn(total, value) { total + value })
  |> should.equal(Ok(160))
}

pub fn eager_error_short_circuits_mapping_test() {
  let sequence: eager.Eager(Int, String) =
    eager.fail("boom")
    |> eager.map(fn(value) { value * 10 })
    |> eager.map_error(fn(reason) { "wrapped:" <> reason })

  sequence
  |> eager.to_result
  |> should.equal(Error("wrapped:boom"))
}

pub fn eager_to_actor_backed_observable_bridge_test() {
  let events = process.new_subject()
  let assert Ok(runtime_) = runtime.start()

  let stream: rx.Observable(Int, String) =
    eager.from_list([2, 3, 4])
    |> eager.map(fn(value) { value * 10 })
    |> eager.to_observable
    |> rx.filter(fn(value) { value >= 30 })

  let assert Ok(_subscription) =
    rx.subscribe(
      stream,
      runtime_,
      rx.observer(
        fn(value) { process.send(events, Value(value)) },
        fn(reason) { process.send(events, Failed(reason)) },
        fn() { process.send(events, Completed) },
      ),
    )

  receive(events) |> should.equal(Value(30))
  receive(events) |> should.equal(Value(40))
  receive(events) |> should.equal(Completed)

  runtime.stop(runtime_)
}

fn receive(events: process.Subject(Event)) -> Event {
  let assert Ok(event) = process.receive(from: events, within: 1000)
  event
}

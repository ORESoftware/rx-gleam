import gleam/erlang/process
import gleeunit
import gleeunit/should
import rx
import rx/flow
import rx/future
import rx/runtime

pub type OutputEvent {
  Value(Int)
  Failed(String)
  Completed
}

pub type ControlledWork {
  ControlledWork(Int, fn(Result(Int, String)) -> Nil)
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn cookbook_01_emit_one_value_test() {
  assert_stream(rx.of(7), [Value(7), Completed])
}

pub fn cookbook_02_emit_finite_list_test() {
  assert_stream(rx.from_list([1, 2, 3]), [
    Value(1),
    Value(2),
    Value(3),
    Completed,
  ])
}

pub fn cookbook_03_complete_without_values_test() {
  let source: rx.Observable(Int, String) = rx.empty()
  assert_stream(source, [Completed])
}

pub fn cookbook_04_fail_immediately_test() {
  let source: rx.Observable(Int, String) = rx.fail("boom")
  assert_stream(source, [Failed("boom")])
}

pub fn cookbook_05_transform_values_test() {
  rx.from_list([1, 2, 3])
  |> rx.map(fn(value) { value * 10 })
  |> assert_stream([Value(10), Value(20), Value(30), Completed])
}

pub fn cookbook_06_filter_values_test() {
  rx.from_list([1, 2, 3, 4])
  |> rx.filter(fn(value) { value % 2 == 0 })
  |> assert_stream([Value(2), Value(4), Completed])
}

pub fn cookbook_07_tap_inspects_and_forwards_test() {
  let inspected = process.new_subject()
  let source: rx.Observable(Int, String) =
    rx.of(9)
    |> rx.tap(fn(value) { process.send(inspected, value) })

  assert_stream(source, [Value(9), Completed])
  process.receive(from: inspected, within: 1000)
  |> should.equal(Ok(9))
}

pub fn cookbook_08_explicit_observer_handlers_test() {
  let values = process.new_subject()
  let errors = process.new_subject()
  let completions = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) = rx.of(5)
  let observer =
    rx.observer(
      fn(value) { process.send(values, value) },
      fn(reason) { process.send(errors, reason) },
      fn() { process.send(completions, Nil) },
    )

  let assert Ok(subscription) = rx.subscribe(source, runtime_, observer)
  process.receive(from: values, within: 1000) |> should.equal(Ok(5))
  process.receive(from: completions, within: 1000) |> should.equal(Ok(Nil))
  process.receive(from: errors, within: 20) |> should.equal(Error(Nil))

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn cookbook_09_cancellation_is_idempotent_test() {
  let teardowns = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.create(fn(_) { fn() { process.send(teardowns, Nil) } })
  let observer = rx.observer(fn(_) { Nil }, fn(_) { Nil }, fn() { Nil })
  let assert Ok(subscription) = rx.subscribe(source, runtime_, observer)

  rx.unsubscribe(subscription)
  rx.unsubscribe(subscription)

  process.receive(from: teardowns, within: 1000) |> should.equal(Ok(Nil))
  process.receive(from: teardowns, within: 20) |> should.equal(Error(Nil))
  runtime.stop(runtime_)
}

pub fn cookbook_10_future_pure_test() {
  let results = process.new_subject()
  let value: future.Future(Int, String) = future.pure(42)
  let _cancel = future.run(value, fn(result) { process.send(results, result) })

  process.receive(from: results, within: 1000)
  |> should.equal(Ok(Ok(42)))
}

pub fn cookbook_11_future_fail_test() {
  let results = process.new_subject()
  let value: future.Future(Int, String) = future.fail("nope")
  let _cancel = future.run(value, fn(result) { process.send(results, result) })

  process.receive(from: results, within: 1000)
  |> should.equal(Ok(Error("nope")))
}

pub fn cookbook_12_future_map_test() {
  let results = process.new_subject()
  let value: future.Future(Int, String) =
    future.pure(4)
    |> future.map(fn(number) { number * 3 })
  let _cancel = future.run(value, fn(result) { process.send(results, result) })

  process.receive(from: results, within: 1000)
  |> should.equal(Ok(Ok(12)))
}

pub fn cookbook_13_future_await_continuation_test() {
  let results = process.new_subject()
  let value: future.Future(Int, String) = future.pure(8)
  let _cancel =
    future.await(value, fn(result) { process.send(results, result) })

  process.receive(from: results, within: 1000)
  |> should.equal(Ok(Ok(8)))
}

pub fn cookbook_14_future_to_stream_test() {
  let value: future.Future(Int, String) = future.pure(11)
  value
  |> flow.from_future
  |> assert_stream([Value(11), Completed])
}

pub fn cookbook_15_async_source_can_emit_later_test() {
  let emitters = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.create(fn(emitter) {
      process.send(emitters, emitter)
      fn() { Nil }
    })
  let assert Ok(subscription) =
    rx.subscribe(source, runtime_, output_observer(outputs))
  let assert Ok(emitter) = process.receive(from: emitters, within: 1000)

  process.receive(from: outputs, within: 20) |> should.equal(Error(Nil))
  rx.next(emitter, 21)
  rx.complete(emitter)
  receive_output(outputs) |> should.equal(Value(21))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn cookbook_16_concat_map_is_fifo_test() {
  let work = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3])
    |> flow.concat_map(fn(value) {
      controlled_future(value, work, process.new_subject())
    })
  let assert Ok(subscription) =
    rx.subscribe(source, runtime_, output_observer(outputs))

  let assert Ok(ControlledWork(1, resolve1)) =
    process.receive(from: work, within: 1000)
  process.receive(from: work, within: 20) |> should.equal(Error(Nil))

  resolve1(Ok(10))
  let assert Ok(ControlledWork(2, resolve2)) =
    process.receive(from: work, within: 1000)
  receive_output(outputs) |> should.equal(Value(10))

  resolve2(Ok(20))
  let assert Ok(ControlledWork(3, resolve3)) =
    process.receive(from: work, within: 1000)
  receive_output(outputs) |> should.equal(Value(20))

  resolve3(Ok(30))
  receive_output(outputs) |> should.equal(Value(30))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn cookbook_17_merge_map_bounds_concurrency_test() {
  let work = process.new_subject()
  let cancelled = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3])
    |> flow.merge_map(
      fn(value) { controlled_future(value, work, cancelled) },
      2,
    )
  let assert Ok(subscription) =
    rx.subscribe(source, runtime_, output_observer(outputs))

  let assert Ok(ControlledWork(1, resolve1)) =
    process.receive(from: work, within: 1000)
  let assert Ok(ControlledWork(2, resolve2)) =
    process.receive(from: work, within: 1000)
  process.receive(from: work, within: 20) |> should.equal(Error(Nil))

  resolve2(Ok(20))
  let assert Ok(ControlledWork(3, resolve3)) =
    process.receive(from: work, within: 1000)
  receive_output(outputs) |> should.equal(Value(20))

  resolve1(Ok(10))
  receive_output(outputs) |> should.equal(Value(10))
  resolve3(Ok(30))
  receive_output(outputs) |> should.equal(Value(30))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn cookbook_18_map_ordered_buffers_early_results_test() {
  let work = process.new_subject()
  let cancelled = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.from_list([1, 2])
    |> flow.map_ordered(
      fn(value) { controlled_future(value, work, cancelled) },
      2,
    )
  let assert Ok(subscription) =
    rx.subscribe(source, runtime_, output_observer(outputs))

  let assert Ok(ControlledWork(1, resolve1)) =
    process.receive(from: work, within: 1000)
  let assert Ok(ControlledWork(2, resolve2)) =
    process.receive(from: work, within: 1000)

  resolve2(Ok(20))
  process.receive(from: outputs, within: 20) |> should.equal(Error(Nil))
  resolve1(Ok(10))
  receive_output(outputs) |> should.equal(Value(10))
  receive_output(outputs) |> should.equal(Value(20))
  receive_output(outputs) |> should.equal(Completed)

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

pub fn cookbook_19_async_filter_preserves_order_test() {
  let source: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3, 4])
    |> flow.filter_async_concurrent(
      fn(value) { future.pure(value % 2 == 0) },
      2,
    )

  assert_stream(source, [Value(2), Value(4), Completed])
}

pub fn cookbook_20_fail_fast_cancels_and_ignores_late_result_test() {
  let work = process.new_subject()
  let cancelled = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3])
    |> flow.merge_map(
      fn(value) { controlled_future(value, work, cancelled) },
      2,
    )
  let assert Ok(subscription) =
    rx.subscribe(source, runtime_, output_observer(outputs))

  let assert Ok(ControlledWork(1, resolve1)) =
    process.receive(from: work, within: 1000)
  let assert Ok(ControlledWork(2, resolve2)) =
    process.receive(from: work, within: 1000)
  process.receive(from: work, within: 20) |> should.equal(Error(Nil))

  resolve2(Error("boom"))
  receive_output(outputs) |> should.equal(Failed("boom"))
  process.receive(from: cancelled, within: 1000) |> should.equal(Ok(1))

  resolve1(Ok(10))
  process.receive(from: outputs, within: 20) |> should.equal(Error(Nil))
  process.receive(from: work, within: 20) |> should.equal(Error(Nil))

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

fn controlled_future(
  value: Int,
  work: process.Subject(ControlledWork),
  cancelled: process.Subject(Int),
) -> future.Future(Int, String) {
  future.new(fn(resolve) {
    process.send(work, ControlledWork(value, resolve))
    fn() { process.send(cancelled, value) }
  })
}

fn assert_stream(
  source: rx.Observable(Int, String),
  expected: List(OutputEvent),
) -> Nil {
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let assert Ok(subscription) =
    rx.subscribe(source, runtime_, output_observer(outputs))

  expect_outputs(outputs, expected)
  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}

fn expect_outputs(
  outputs: process.Subject(OutputEvent),
  expected: List(OutputEvent),
) -> Nil {
  case expected {
    [] -> Nil
    [first, ..rest] -> {
      receive_output(outputs) |> should.equal(first)
      expect_outputs(outputs, rest)
    }
  }
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

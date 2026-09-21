import gleam/list
import gleeunit
import gleeunit/should
import rx
import rx/eager

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn lazy_map_filter_take_test() {
  let stream: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3, 4, 5])
    |> rx.map(fn(value) { value * 2 })
    |> rx.filter(fn(value) { value > 4 })
    |> rx.take(2)

  stream
  |> rx.to_list
  |> should.equal(Ok([6, 8]))
}

pub fn lazy_scan_test() {
  let stream: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3, 4])
    |> rx.scan(0, fn(total, value) { total + value })

  stream
  |> rx.to_list
  |> should.equal(Ok([1, 3, 6, 10]))
}

pub fn lazy_start_with_and_distinct_test() {
  let stream: rx.Observable(Int, String) =
    rx.from_list([2, 2, 3, 3, 4])
    |> rx.start_with([1, 1])
    |> rx.distinct_until_changed_by(fn(left, right) { left == right })

  stream
  |> rx.to_list
  |> should.equal(Ok([1, 2, 3, 4]))
}

pub fn lazy_failure_test() {
  let stream: rx.Observable(Int, String) = rx.fail("boom")

  stream
  |> rx.to_list
  |> should.equal(Error("boom"))
}

pub fn lazy_from_result_test() {
  let ok_stream: rx.Observable(Int, String) = rx.from_result(Ok(7))
  let error_stream: rx.Observable(Int, String) = rx.from_result(Error("nope"))

  ok_stream |> rx.to_list |> should.equal(Ok([7]))
  error_stream |> rx.to_list |> should.equal(Error("nope"))
}

pub fn lazy_map_error_test() {
  let stream: rx.Observable(Int, Int) =
    rx.fail("boom")
    |> rx.map_error(fn(error) {
      case error {
        "boom" -> 42
        _ -> 0
      }
    })

  stream |> rx.to_list |> should.equal(Error(42))
}

pub fn lazy_take_zero_does_not_open_upstream_test() {
  let stream: rx.Observable(Int, String) =
    rx.defer(fn() { panic as "take(0) opened its upstream" })

  stream
  |> rx.take(0)
  |> rx.to_list
  |> should.equal(Ok([]))
}

pub fn lazy_take_negative_does_not_open_upstream_test() {
  let stream: rx.Observable(Int, String) =
    rx.defer(fn() { panic as "take(-1) opened its upstream" })

  stream
  |> rx.take(-1)
  |> rx.to_list
  |> should.equal(Ok([]))
}

pub fn lazy_skip_non_positive_is_noop_test() {
  let stream: rx.Observable(Int, String) = rx.from_list([1, 2, 3])

  stream |> rx.skip(0) |> rx.to_list |> should.equal(Ok([1, 2, 3]))
  stream |> rx.skip(-10) |> rx.to_list |> should.equal(Ok([1, 2, 3]))
}

pub fn lazy_subscription_is_cold_test() {
  let stream: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3])
    |> rx.scan(0, fn(total, value) { total + value })

  stream |> rx.to_list |> should.equal(Ok([1, 3, 6]))
  stream |> rx.to_list |> should.equal(Ok([1, 3, 6]))
}

pub fn subscribe_stop_does_not_pull_again_test() {
  let stream: rx.Observable(Int, String) =
    rx.create(fn() {
      rx.source(
        fn() {
          rx.Emit(1, fn() { panic as "subscription pulled after Stop" })
        },
        fn() { Nil },
      )
    })

  rx.subscribe(
    stream,
    rx.observer(
      fn(value) {
        value |> should.equal(1)
        rx.Stop
      },
      fn(_) { panic as "unexpected error callback" },
      fn() { panic as "unexpected completion callback after Stop" },
    ),
  )
}

pub fn subscribe_failure_does_not_complete_test() {
  let stream: rx.Observable(Int, String) = rx.fail("boom")

  rx.subscribe(
    stream,
    rx.observer(
      fn(_) { panic as "unexpected value callback" },
      fn(error) { error |> should.equal("boom") },
      fn() { panic as "failure also invoked completion" },
    ),
  )
}

pub fn low_level_source_is_composable_test() {
  let stream: rx.Observable(Int, String) =
    rx.create(fn() {
      rx.source(fn() { rx.Emit(21, fn() { rx.Complete }) }, fn() { Nil })
    })
    |> rx.map(fn(value) { value * 2 })

  stream
  |> rx.to_list
  |> should.equal(Ok([42]))
}

pub fn lazy_fold_and_drain_test() {
  let stream: rx.Observable(Int, String) = rx.from_list([1, 2, 3, 4])

  stream
  |> rx.fold(0, fn(total, value) { total + value })
  |> should.equal(Ok(10))

  stream |> rx.drain |> should.equal(Ok(Nil))
}

pub fn lazy_large_sequence_test() {
  let values = range(25_000)
  let stream: rx.Observable(Int, String) =
    rx.from_list(values)
    |> rx.map(fn(value) { value + 1 })
    |> rx.filter(fn(value) { value > 0 })

  let assert Ok(output) = rx.to_list(stream)
  output |> list.length |> should.equal(25_000)
}

pub fn lazy_large_distinct_run_test() {
  let values = repeat(25_000, 1, [])
  let stream: rx.Observable(Int, String) =
    rx.from_list(values)
    |> rx.distinct_until_changed_by(fn(left, right) { left == right })

  stream |> rx.to_list |> should.equal(Ok([1]))
}

pub fn eager_pipeline_test() {
  let sequence: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3, 4, 5])
    |> eager.map(fn(value) { value * 3 })
    |> eager.filter(fn(value) { value > 6 })
    |> eager.take(2)

  sequence
  |> eager.to_result
  |> should.equal(Ok([9, 12]))
}

pub fn eager_scan_test() {
  let sequence: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3])
    |> eager.scan(10, fn(total, value) { total + value })

  sequence
  |> eager.to_result
  |> should.equal(Ok([11, 13, 16]))
}

pub fn eager_flat_map_test() {
  let sequence: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3])
    |> eager.flat_map(fn(value) { eager.from_list([value, value * 10]) })

  sequence
  |> eager.to_result
  |> should.equal(Ok([1, 10, 2, 20, 3, 30]))
}

pub fn eager_append_preserves_order_test() {
  let first: eager.Eager(Int, String) = eager.from_list([1, 2, 3])
  let second: eager.Eager(Int, String) = eager.from_list([4, 5])

  first
  |> eager.append(second)
  |> eager.to_result
  |> should.equal(Ok([1, 2, 3, 4, 5]))
}

pub fn eager_from_result_and_map_error_test() {
  let ok_sequence: eager.Eager(Int, String) = eager.from_result(Ok(7))
  let error_sequence: eager.Eager(Int, Int) =
    eager.from_result(Error("boom"))
    |> eager.map_error(fn(error) {
      case error {
        "boom" -> 42
        _ -> 0
      }
    })

  ok_sequence |> eager.to_result |> should.equal(Ok([7]))
  error_sequence |> eager.to_result |> should.equal(Error(42))
}

pub fn eager_error_short_circuits_pipeline_test() {
  let sequence: eager.Eager(Int, String) =
    eager.fail("boom")
    |> eager.map(fn(value) { value * 2 })
    |> eager.filter(fn(value) { value > 0 })
    |> eager.take(2)

  sequence |> eager.to_result |> should.equal(Error("boom"))
}

pub fn eager_to_lazy_round_trip_test() {
  let sequence: eager.Eager(Int, String) = eager.from_list([3, 4, 5])

  sequence
  |> eager.to_observable
  |> rx.map(fn(value) { value + 1 })
  |> rx.to_list
  |> should.equal(Ok([4, 5, 6]))
}

pub fn eager_large_sequence_stack_safety_test() {
  let values = range(25_000)
  let sequence: eager.Eager(Int, String) =
    eager.from_list(values)
    |> eager.map(fn(value) { value + 1 })
    |> eager.filter(fn(value) { value > 0 })
    |> eager.take(25_000)

  let assert Ok(output) = eager.to_result(sequence)
  output |> list.length |> should.equal(25_000)
}

pub fn eager_large_append_stack_safety_test() {
  let first: eager.Eager(Int, String) = eager.from_list(range(12_500))
  let second: eager.Eager(Int, String) = eager.from_list(range(12_500))

  let assert Ok(output) = first |> eager.append(second) |> eager.to_result
  output |> list.length |> should.equal(25_000)
}

pub fn eager_large_flat_map_stack_safety_test() {
  let sequence: eager.Eager(Int, String) =
    eager.from_list(range(10_000))
    |> eager.flat_map(fn(value) { eager.from_list([value, value + 1]) })

  let assert Ok(output) = eager.to_result(sequence)
  output |> list.length |> should.equal(20_000)
}

fn range(count: Int) -> List(Int) {
  range_loop(1, count, []) |> list.reverse
}

fn range_loop(current: Int, count: Int, reversed: List(Int)) -> List(Int) {
  case current > count {
    True -> reversed
    False -> range_loop(current + 1, count, [current, ..reversed])
  }
}

fn repeat(count: Int, value: Int, reversed: List(Int)) -> List(Int) {
  case count <= 0 {
    True -> reversed
    False -> repeat(count - 1, value, [value, ..reversed])
  }
}

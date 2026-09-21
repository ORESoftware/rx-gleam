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

pub fn low_level_source_is_composable_test() {
  let stream: rx.Observable(Int, String) =
    rx.create(fn() {
      rx.source(
        fn() { rx.Emit(21, fn() { rx.Complete }) },
        fn() { Nil },
      )
    })
    |> rx.map(fn(value) { value * 2 })

  stream
  |> rx.to_list
  |> should.equal(Ok([42]))
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

pub fn eager_to_lazy_round_trip_test() {
  let sequence: eager.Eager(Int, String) = eager.from_list([3, 4, 5])

  sequence
  |> eager.to_observable
  |> rx.map(fn(value) { value + 1 })
  |> rx.to_list
  |> should.equal(Ok([4, 5, 6]))
}

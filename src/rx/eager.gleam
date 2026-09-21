import gleam/list
import rx

/// A finite reactive sequence evaluated eagerly.
///
/// Operators on `Eager` run immediately and remain composable with Gleam's
/// pipeline operator. Convert to the actor-backed Observable API with
/// `to_observable` when asynchronous/runtime-managed composition is needed.
pub opaque type Eager(value, error) {
  Eager(Result(List(value), error))
}

pub fn from_list(values: List(value)) -> Eager(value, error) {
  Eager(Ok(values))
}

pub fn from_result(result: Result(value, error)) -> Eager(value, error) {
  case result {
    Ok(value) -> of(value)
    Error(error) -> fail(error)
  }
}

pub fn of(value: value) -> Eager(value, error) {
  from_list([value])
}

pub fn empty() -> Eager(value, error) {
  from_list([])
}

pub fn fail(error: error) -> Eager(value, error) {
  Eager(Error(error))
}

/// Convert an eager sequence into the current actor-backed Observable API.
pub fn to_observable(
  sequence: Eager(value, error),
) -> rx.Observable(value, error) {
  case sequence {
    Eager(Ok(values)) -> rx.from_list(values)
    Eager(Error(error)) -> rx.fail(error)
  }
}

pub fn to_result(sequence: Eager(value, error)) -> Result(List(value), error) {
  let Eager(result) = sequence
  result
}

pub fn map(sequence: Eager(a, error), mapper: fn(a) -> b) -> Eager(b, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(map_list(values, mapper)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

pub fn map_error(
  sequence: Eager(value, a),
  mapper: fn(a) -> b,
) -> Eager(value, b) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(values))
    Eager(Error(error)) -> Eager(Error(mapper(error)))
  }
}

pub fn filter(
  sequence: Eager(value, error),
  predicate: fn(value) -> Bool,
) -> Eager(value, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(filter_list(values, predicate)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

pub fn tap(
  sequence: Eager(value, error),
  effect: fn(value) -> Nil,
) -> Eager(value, error) {
  map(sequence, fn(value) {
    effect(value)
    value
  })
}

pub fn take(sequence: Eager(value, error), count: Int) -> Eager(value, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(take_list(values, count)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

pub fn skip(sequence: Eager(value, error), count: Int) -> Eager(value, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(skip_list(values, count)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

pub fn scan(
  sequence: Eager(value, error),
  initial: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> Eager(accumulator, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(scan_list(values, initial, reducer, [])))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

pub fn flat_map(
  sequence: Eager(a, error),
  mapper: fn(a) -> Eager(b, error),
) -> Eager(b, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(flat_map_list(values, mapper, []))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

pub fn append(
  first: Eager(value, error),
  second: Eager(value, error),
) -> Eager(value, error) {
  case first {
    Eager(Error(error)) -> Eager(Error(error))
    Eager(Ok(left)) ->
      case second {
        Eager(Error(error)) -> Eager(Error(error))
        Eager(Ok(right)) -> Eager(Ok(append_lists(left, right)))
      }
  }
}

pub fn fold(
  sequence: Eager(value, error),
  initial: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> Result(accumulator, error) {
  case sequence {
    Eager(Error(error)) -> Error(error)
    Eager(Ok(values)) -> Ok(fold_list(values, initial, reducer))
  }
}

fn map_list(values: List(a), mapper: fn(a) -> b) -> List(b) {
  map_list_loop(values, mapper, []) |> list.reverse
}

fn map_list_loop(
  values: List(a),
  mapper: fn(a) -> b,
  reversed: List(b),
) -> List(b) {
  case values {
    [] -> reversed
    [first, ..rest] -> map_list_loop(rest, mapper, [mapper(first), ..reversed])
  }
}

fn filter_list(
  values: List(value),
  predicate: fn(value) -> Bool,
) -> List(value) {
  filter_list_loop(values, predicate, []) |> list.reverse
}

fn filter_list_loop(
  values: List(value),
  predicate: fn(value) -> Bool,
  reversed: List(value),
) -> List(value) {
  case values {
    [] -> reversed
    [first, ..rest] ->
      case predicate(first) {
        True -> filter_list_loop(rest, predicate, [first, ..reversed])
        False -> filter_list_loop(rest, predicate, reversed)
      }
  }
}

fn take_list(values: List(value), count: Int) -> List(value) {
  take_list_loop(values, count, []) |> list.reverse
}

fn take_list_loop(
  values: List(value),
  count: Int,
  reversed: List(value),
) -> List(value) {
  case count <= 0 {
    True -> reversed
    False ->
      case values {
        [] -> reversed
        [first, ..rest] -> take_list_loop(rest, count - 1, [first, ..reversed])
      }
  }
}

fn skip_list(values: List(value), count: Int) -> List(value) {
  case count <= 0 {
    True -> values
    False ->
      case values {
        [] -> []
        [_, ..rest] -> skip_list(rest, count - 1)
      }
  }
}

fn scan_list(
  values: List(value),
  accumulator: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
  reversed: List(accumulator),
) -> List(accumulator) {
  case values {
    [] -> list.reverse(reversed)
    [first, ..rest] -> {
      let updated = reducer(accumulator, first)
      scan_list(rest, updated, reducer, [updated, ..reversed])
    }
  }
}

fn flat_map_list(
  values: List(a),
  mapper: fn(a) -> Eager(b, error),
  reversed: List(b),
) -> Result(List(b), error) {
  case values {
    [] -> Ok(list.reverse(reversed))
    [first, ..rest] ->
      case mapper(first) {
        Eager(Error(error)) -> Error(error)
        Eager(Ok(mapped)) ->
          flat_map_list(rest, mapper, prepend_to_reversed(mapped, reversed))
      }
  }
}

fn prepend_to_reversed(
  values: List(value),
  reversed: List(value),
) -> List(value) {
  case values {
    [] -> reversed
    [first, ..rest] -> prepend_to_reversed(rest, [first, ..reversed])
  }
}

fn append_lists(first: List(value), second: List(value)) -> List(value) {
  prepend_reversed(list.reverse(first), second)
}

fn prepend_reversed(reversed: List(value), tail: List(value)) -> List(value) {
  case reversed {
    [] -> tail
    [first, ..rest] -> prepend_reversed(rest, [first, ..tail])
  }
}

fn fold_list(
  values: List(value),
  accumulator: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> accumulator {
  case values {
    [] -> accumulator
    [first, ..rest] -> fold_list(rest, reducer(accumulator, first), reducer)
  }
}

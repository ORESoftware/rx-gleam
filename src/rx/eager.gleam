import gleam/list
import rx

/// A finite reactive sequence that is evaluated eagerly.
///
/// Unlike `rx.Observable`, operators on `Eager` run immediately. The value is
/// still composable with Gleam's pipeline operator and can be converted to a
/// lazy observable at any point.
pub opaque type Eager(value, error) {
  Eager(Result(List(value), error))
}

/// Create an eager sequence from already-available values.
pub fn from_list(values: List(value)) -> Eager(value, error) {
  Eager(Ok(values))
}

/// Create a one-value eager sequence.
pub fn of(value: value) -> Eager(value, error) {
  from_list([value])
}

/// Create an empty eager sequence.
pub fn empty() -> Eager(value, error) {
  from_list([])
}

/// Create an eager failed sequence.
pub fn fail(error: error) -> Eager(value, error) {
  Eager(Error(error))
}

/// Materialize a lazy observable immediately.
pub fn from_observable(
  observable: rx.Observable(value, error),
) -> Eager(value, error) {
  Eager(rx.to_list(observable))
}

/// Convert an eager sequence to a cold observable.
pub fn to_observable(
  sequence: Eager(value, error),
) -> rx.Observable(value, error) {
  case sequence {
    Eager(Ok(values)) -> rx.from_list(values)
    Eager(Error(error)) -> rx.fail(error)
  }
}

/// Delay creation of an eager sequence until a lazy subscription starts.
pub fn defer(
  factory: fn() -> Eager(value, error),
) -> rx.Observable(value, error) {
  rx.defer(fn() { to_observable(factory()) })
}

/// Extract the materialized result.
pub fn to_result(sequence: Eager(value, error)) -> Result(List(value), error) {
  let Eager(result) = sequence
  result
}

/// Transform all values immediately.
pub fn map(sequence: Eager(a, error), mapper: fn(a) -> b) -> Eager(b, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(map_list(values, mapper)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

/// Retain matching values immediately.
pub fn filter(
  sequence: Eager(value, error),
  predicate: fn(value) -> Bool,
) -> Eager(value, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(filter_list(values, predicate)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

/// Run an effect immediately for each value while preserving the values.
pub fn tap(
  sequence: Eager(value, error),
  effect: fn(value) -> Nil,
) -> Eager(value, error) {
  map(sequence, fn(value) {
    effect(value)
    value
  })
}

/// Keep at most `count` values immediately.
pub fn take(sequence: Eager(value, error), count: Int) -> Eager(value, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(take_list(values, count)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

/// Drop the first `count` values immediately.
pub fn skip(sequence: Eager(value, error), count: Int) -> Eager(value, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(Ok(skip_list(values, count)))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

/// Emit each running accumulator immediately.
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

/// Map each value to another eager sequence and concatenate the results.
pub fn flat_map(
  sequence: Eager(a, error),
  mapper: fn(a) -> Eager(b, error),
) -> Eager(b, error) {
  case sequence {
    Eager(Ok(values)) -> Eager(flat_map_list(values, mapper))
    Eager(Error(error)) -> Eager(Error(error))
  }
}

/// Append another eager sequence.
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

/// Reduce the eager values to a single result.
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
  case values {
    [] -> []
    [first, ..rest] -> [mapper(first), ..map_list(rest, mapper)]
  }
}

fn filter_list(
  values: List(value),
  predicate: fn(value) -> Bool,
) -> List(value) {
  case values {
    [] -> []
    [first, ..rest] -> {
      let filtered_rest = filter_list(rest, predicate)
      case predicate(first) {
        True -> [first, ..filtered_rest]
        False -> filtered_rest
      }
    }
  }
}

fn take_list(values: List(value), count: Int) -> List(value) {
  case count <= 0 {
    True -> []
    False ->
      case values {
        [] -> []
        [first, ..rest] -> [first, ..take_list(rest, count - 1)]
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
) -> Result(List(b), error) {
  case values {
    [] -> Ok([])
    [first, ..rest] -> {
      case mapper(first) {
        Eager(Error(error)) -> Error(error)
        Eager(Ok(mapped)) ->
          case flat_map_list(rest, mapper) {
            Error(error) -> Error(error)
            Ok(tail) -> Ok(append_lists(mapped, tail))
          }
      }
    }
  }
}

fn append_lists(first: List(value), second: List(value)) -> List(value) {
  case first {
    [] -> second
    [head, ..tail] -> [head, ..append_lists(tail, second)]
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

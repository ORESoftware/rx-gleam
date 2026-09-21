import rx
import rx/future
import rx/runtime

/// Strict FIFO async mapping.
///
/// Source values may arrive at arbitrary future times. At most one projected
/// Future runs at once; later source values remain queued until the active
/// Future resolves. This is the ReactiveX `concatMap` scheduling policy.
pub fn concat_map(
  source: rx.Observable(input, error),
  project: fn(input) -> future.Future(output, error),
) -> rx.Observable(output, error) {
  map_concurrent(source, project, 1, runtime.InputOrder)
}

/// Concurrent async mapping with FIFO dispatch and completion-order emission.
///
/// `concurrency` bounds the number of projected Futures that can be active at
/// once. New source values continue arriving asynchronously and are buffered in
/// FIFO order until a slot becomes available.
pub fn merge_map(
  source: rx.Observable(input, error),
  project: fn(input) -> future.Future(output, error),
  concurrency: Int,
) -> rx.Observable(output, error) {
  map_concurrent(source, project, concurrency, runtime.CompletionOrder)
}

/// Concurrent async mapping that preserves input order at emission time.
///
/// Work may finish out of order, but successful results are buffered until all
/// earlier input sequence numbers have emitted.
pub fn map_ordered(
  source: rx.Observable(input, error),
  project: fn(input) -> future.Future(output, error),
  concurrency: Int,
) -> rx.Observable(output, error) {
  map_concurrent(source, project, concurrency, runtime.InputOrder)
}

/// Async map with exactly one operation in flight.
pub fn map_async(
  source: rx.Observable(input, error),
  project: fn(input) -> future.Future(output, error),
) -> rx.Observable(output, error) {
  concat_map(source, project)
}

/// Async filter with exactly one predicate Future in flight.
pub fn filter_async(
  source: rx.Observable(value, error),
  predicate: fn(value) -> future.Future(Bool, error),
) -> rx.Observable(value, error) {
  filter_async_concurrent(source, predicate, 1)
}

/// Evaluate async predicates concurrently while preserving source order.
pub fn filter_async_concurrent(
  source: rx.Observable(value, error),
  predicate: fn(value) -> future.Future(Bool, error),
  concurrency: Int,
) -> rx.Observable(value, error) {
  let decisions =
    map_ordered(
      source,
      fn(value) {
        future.map(predicate(value), fn(keep) {
          Decision(value: value, keep: keep)
        })
      },
      concurrency,
    )

  decisions
  |> rx.filter(fn(decision) { decision.keep })
  |> rx.map(fn(decision) { decision.value })
}

/// Convert one Future into a single-value Observable.
pub fn from_future(
  future_: future.Future(value, error),
) -> rx.Observable(value, error) {
  rx.create(fn(emitter) {
    future.run(future_, fn(result) {
      case result {
        Ok(value) -> {
          rx.next(emitter, value)
          rx.complete(emitter)
        }
        Error(reason) -> rx.error(emitter, reason)
      }
    })
  })
}

type Decision(value) {
  Decision(value: value, keep: Bool)
}

fn map_concurrent(
  source: rx.Observable(input, error),
  project: fn(input) -> future.Future(output, error),
  concurrency: Int,
  order: runtime.FlowOrder,
) -> rx.Observable(output, error) {
  rx.create_checked(fn(runtime_, emitter) {
    case runtime.register_flow(
      runtime_,
      concurrency,
      order,
      fn() { rx.complete(emitter) },
    ) {
      Error(reason) -> Error(reason)
      Ok(flow_key) -> {
        let upstream =
          rx.subscribe(
            source,
            runtime_,
            rx.observer(
              fn(value) {
                runtime.enqueue_flow(runtime_, flow_key, fn(sequence) {
                  future.run(project(value), fn(result) {
                    case result {
                      Ok(output) ->
                        runtime.complete_flow(
                          runtime_,
                          flow_key,
                          sequence,
                          runtime.FlowSuccess,
                          fn() { rx.next(emitter, output) },
                        )
                      Error(reason) ->
                        runtime.complete_flow(
                          runtime_,
                          flow_key,
                          sequence,
                          runtime.FlowFailure,
                          fn() { rx.error(emitter, reason) },
                        )
                    }
                  })
                })
              },
              fn(reason) {
                runtime.fail_flow_input(
                  runtime_,
                  flow_key,
                  fn() { rx.error(emitter, reason) },
                )
              },
              fn() { runtime.finish_flow_input(runtime_, flow_key) },
            ),
          )

        case upstream {
          Error(reason) -> {
            runtime.cancel_flow(runtime_, flow_key)
            Error(reason)
          }
          Ok(subscription) ->
            Ok(fn() {
              rx.unsubscribe(subscription)
              runtime.cancel_flow(runtime_, flow_key)
            })
        }
      }
    }
  })
}

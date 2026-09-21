import rx/protocol.{type Notification, Complete, Error, Next}
import rx/runtime.{type Runtime, type RuntimeError}

pub type Observer(value, error) {
  Observer(
    on_next: fn(value) -> Nil,
    on_error: fn(error) -> Nil,
    on_complete: fn() -> Nil,
  )
}

pub opaque type Subscription {
  Subscription(
    runtime_: Runtime,
    key: runtime.SubscriptionKey,
  )
}

pub opaque type Observable(value, error) {
  Observable(
    subscribe_: fn(Runtime, Observer(value, error)) ->
      Result(Subscription, RuntimeError),
  )
}

pub type Emitter(value, error) {
  Emitter(emit: fn(Notification(value, error)) -> Nil)
}

pub fn observer(
  on_next: fn(value) -> Nil,
  on_error: fn(error) -> Nil,
  on_complete: fn() -> Nil,
) -> Observer(value, error) {
  Observer(on_next:, on_error:, on_complete:)
}

pub fn create(
  producer: fn(Emitter(value, error)) -> fn() -> Nil,
) -> Observable(value, error) {
  Observable(fn(runtime_, observer_) {
    case runtime.register(runtime_) {
      Error(reason) -> Error(reason)
      Ok(key) -> {
        let emitter = Emitter(fn(notification) {
          runtime.dispatch(
            runtime_,
            key,
            protocol.kind(notification),
            fn() { notify(observer_, notification) },
          )
        })
        let teardown = producer(emitter)
        runtime.set_teardown(runtime_, key, teardown)
        Ok(Subscription(runtime_: runtime_, key: key))
      }
    }
  })
}

pub fn subscribe(
  observable: Observable(value, error),
  runtime_: Runtime,
  observer_: Observer(value, error),
) -> Result(Subscription, RuntimeError) {
  let Observable(subscribe_) = observable
  subscribe_(runtime_, observer_)
}

pub fn unsubscribe(subscription: Subscription) -> Nil {
  let Subscription(runtime_: runtime_, key: key) = subscription
  runtime.cancel(runtime_, key)
}

pub fn emit(
  emitter: Emitter(value, error),
  notification: Notification(value, error),
) -> Nil {
  let Emitter(send) = emitter
  send(notification)
}

pub fn next(emitter: Emitter(value, error), value: value) -> Nil {
  emit(emitter, Next(value))
}

pub fn error(emitter: Emitter(value, error), reason: error) -> Nil {
  emit(emitter, Error(reason))
}

pub fn complete(emitter: Emitter(value, error)) -> Nil {
  emit(emitter, Complete)
}

pub fn of(value: value) -> Observable(value, error) {
  from_list([value])
}

pub fn empty() -> Observable(value, error) {
  create(fn(emitter) {
    complete(emitter)
    fn() { Nil }
  })
}

pub fn fail(reason: error) -> Observable(value, error) {
  create(fn(emitter) {
    error(emitter, reason)
    fn() { Nil }
  })
}

pub fn from_list(values: List(value)) -> Observable(value, error) {
  create(fn(emitter) {
    emit_list(emitter, values)
    complete(emitter)
    fn() { Nil }
  })
}

fn emit_list(emitter: Emitter(value, error), values: List(value)) -> Nil {
  case values {
    [] -> Nil
    [first, ..rest] -> {
      next(emitter, first)
      emit_list(emitter, rest)
    }
  }
}

pub fn map(
  observable: Observable(a, error),
  transform: fn(a) -> b,
) -> Observable(b, error) {
  Observable(fn(runtime_, downstream) {
    subscribe(
      observable,
      runtime_,
      Observer(
        on_next: fn(value) { downstream.on_next(transform(value)) },
        on_error: downstream.on_error,
        on_complete: downstream.on_complete,
      ),
    )
  })
}

pub fn filter(
  observable: Observable(value, error),
  predicate: fn(value) -> Bool,
) -> Observable(value, error) {
  Observable(fn(runtime_, downstream) {
    subscribe(
      observable,
      runtime_,
      Observer(
        on_next: fn(value) {
          case predicate(value) {
            True -> downstream.on_next(value)
            False -> Nil
          }
        },
        on_error: downstream.on_error,
        on_complete: downstream.on_complete,
      ),
    )
  })
}

pub fn tap(
  observable: Observable(value, error),
  inspect: fn(value) -> Nil,
) -> Observable(value, error) {
  map(observable, fn(value) {
    inspect(value)
    value
  })
}

fn notify(
  observer_: Observer(value, error),
  notification: Notification(value, error),
) -> Nil {
  case notification {
    Next(value) -> observer_.on_next(value)
    Error(reason) -> observer_.on_error(reason)
    Complete -> observer_.on_complete()
  }
}

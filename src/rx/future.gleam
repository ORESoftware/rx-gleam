import rx/effect

/// A single asynchronous result that may resolve at a later time.
///
/// A `Future` is deliberately execution-agnostic. Its producer may use a BEAM
/// process, timer, socket, database client, FFI callback, or any other mechanism.
/// The returned cancellation function should stop underlying work when possible.
///
/// Producers must resolve at most once. When a Future is used through the
/// reactive flow operators, duplicate or late completions are ignored by the
/// owning flow state machine.
pub opaque type Future(value, error) {
  Future(effect.Effect(value, error))
}

pub fn new(
  start: fn(fn(Result(value, error)) -> Nil) -> fn() -> Nil,
) -> Future(value, error) {
  Future(effect.new(start))
}

pub fn run(
  future: Future(value, error),
  resolve: fn(Result(value, error)) -> Nil,
) -> fn() -> Nil {
  let Future(effect_) = future
  effect.run(effect_, resolve)
}

pub fn pure(value: value) -> Future(value, error) {
  Future(effect.pure(value))
}

pub fn fail(reason: error) -> Future(value, error) {
  Future(effect.fail(reason))
}

pub fn map(
  future: Future(a, error),
  transform: fn(a) -> b,
) -> Future(b, error) {
  let Future(effect_) = future
  Future(effect.map(effect_, transform))
}

pub fn from_effect(effect_: effect.Effect(value, error)) -> Future(value, error) {
  Future(effect_)
}

pub fn to_effect(future: Future(value, error)) -> effect.Effect(value, error) {
  let Future(effect_) = future
  effect_
}

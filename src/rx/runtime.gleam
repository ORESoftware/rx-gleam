import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/erlang/reference
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import rx/protocol

pub opaque type Runtime {
  Runtime(process.Subject(Message))
}

pub opaque type SubscriptionKey {
  SubscriptionKey(reference.Reference)
}

type Entry {
  Entry(
    phase: protocol.Phase,
    teardown: Option(fn() -> Nil),
  )
}

type State {
  State(
    entries: Dict(reference.Reference, Entry),
    on_protocol_error: fn(protocol.ProtocolError) -> Nil,
  )
}

type Message {
  Register(reference.Reference, process.Subject(Nil))
  SetTeardown(reference.Reference, fn() -> Nil)
  Notify(reference.Reference, protocol.Kind, fn() -> Nil)
  Cancel(reference.Reference)
  Stop
}

pub fn start() -> Result(Runtime, actor.StartError) {
  start_checked(fn(_) { Nil })
}

pub fn start_checked(
  on_protocol_error: fn(protocol.ProtocolError) -> Nil,
) -> Result(Runtime, actor.StartError) {
  actor.new(State(entries: dict.new(), on_protocol_error:))
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) {
    let actor.Started(pid: _, data: subject) = started
    Runtime(subject)
  })
}

pub fn register(runtime: Runtime) -> SubscriptionKey {
  let Runtime(subject) = runtime
  let id = reference.new()
  let _ = process.call(
    subject,
    waiting: 5_000,
    sending: fn(reply_to) { Register(id, reply_to) },
  )
  SubscriptionKey(id)
}

pub fn set_teardown(
  runtime: Runtime,
  key: SubscriptionKey,
  teardown: fn() -> Nil,
) -> Nil {
  let Runtime(subject) = runtime
  let SubscriptionKey(id) = key
  process.send(subject, SetTeardown(id, teardown))
}

pub fn dispatch(
  runtime: Runtime,
  key: SubscriptionKey,
  kind: protocol.Kind,
  work: fn() -> Nil,
) -> Nil {
  let Runtime(subject) = runtime
  let SubscriptionKey(id) = key
  process.send(subject, Notify(id, kind, work))
}

pub fn cancel(runtime: Runtime, key: SubscriptionKey) -> Nil {
  let Runtime(subject) = runtime
  let SubscriptionKey(id) = key
  process.send(subject, Cancel(id))
}

pub fn stop(runtime: Runtime) -> Nil {
  let Runtime(subject) = runtime
  process.send(subject, Stop)
}

fn handle_message(state: State, message: Message) -> actor.Next(State, Message) {
  let State(entries:, on_protocol_error:) = state

  case message {
    Register(id, reply_to) -> {
      process.send(reply_to, Nil)
      actor.continue(State(
        entries: dict.insert(
          entries,
          id,
          Entry(phase: protocol.Open, teardown: None),
        ),
        on_protocol_error:,
      ))
    }

    SetTeardown(id, teardown) ->
      case dict.get(entries, id) {
        Error(_) -> {
          teardown()
          actor.continue(state)
        }
        Ok(Entry(phase: protocol.Terminated, teardown: _)) -> {
          teardown()
          actor.continue(State(
            entries: dict.delete(entries, id),
            on_protocol_error:,
          ))
        }
        Ok(Entry(phase: protocol.Open, teardown: _)) ->
          actor.continue(State(
            entries: dict.insert(
              entries,
              id,
              Entry(phase: protocol.Open, teardown: Some(teardown)),
            ),
            on_protocol_error:,
          ))
      }

    Notify(id, kind, work) ->
      case dict.get(entries, id) {
        Error(_) -> actor.continue(state)
        Ok(Entry(phase, teardown)) ->
          case protocol.transition(phase, kind) {
            Error(reason) -> {
              on_protocol_error(reason)
              actor.continue(state)
            }
            Ok(protocol.Open) -> {
              work()
              actor.continue(state)
            }
            Ok(protocol.Terminated) -> {
              work()
              case teardown {
                Some(release) -> {
                  release()
                  actor.continue(State(
                    entries: dict.delete(entries, id),
                    on_protocol_error:,
                  ))
                }
                None ->
                  actor.continue(State(
                    entries: dict.insert(
                      entries,
                      id,
                      Entry(phase: protocol.Terminated, teardown: None),
                    ),
                    on_protocol_error:,
                  ))
              }
            }
          }
      }

    Cancel(id) ->
      case dict.get(entries, id) {
        Error(_) -> actor.continue(state)
        Ok(Entry(phase: _, teardown: teardown)) -> {
          case teardown {
            Some(release) -> release()
            None -> Nil
          }
          actor.continue(State(
            entries: dict.delete(entries, id),
            on_protocol_error:,
          ))
        }
      }

    Stop -> actor.stop()
  }
}

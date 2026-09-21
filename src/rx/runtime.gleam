import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/erlang/reference
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import rx/lifecycle
import rx/protocol

pub opaque type Runtime {
  Runtime(process.Subject(Message))
}

pub opaque type SubscriptionKey {
  SubscriptionKey(reference.Reference)
}

pub type RuntimeError {
  RegistrationTimeout
}

pub type RuntimeDiagnostic {
  ProtocolViolation(protocol.ProtocolError)
  DuplicateTeardownRegistration
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
    on_diagnostic: fn(RuntimeDiagnostic) -> Nil,
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
  on_diagnostic: fn(RuntimeDiagnostic) -> Nil,
) -> Result(Runtime, actor.StartError) {
  actor.new(State(entries: dict.new(), on_diagnostic:))
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) {
    let actor.Started(pid: _, data: subject) = started
    Runtime(subject)
  })
}

pub fn register(runtime: Runtime) -> Result(SubscriptionKey, RuntimeError) {
  let Runtime(subject) = runtime
  let id = reference.new()
  let reply_to = process.new_subject()
  process.send(subject, Register(id, reply_to))

  case process.receive(from: reply_to, within: 5_000) {
    Ok(Nil) -> Ok(SubscriptionKey(id))
    Error(Nil) -> Error(RegistrationTimeout)
  }
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
  let State(entries:, on_diagnostic:) = state

  case message {
    Register(id, reply_to) -> {
      process.send(reply_to, Nil)
      actor.continue(State(
        entries: dict.insert(
          entries,
          id,
          Entry(phase: protocol.Open, teardown: None),
        ),
        on_diagnostic:,
      ))
    }

    SetTeardown(id, incoming) -> {
      let current = dict.get(entries, id)
      let model_state = case current {
        Error(_) -> lifecycle.Closed
        Ok(entry) -> lifecycle_state(entry)
      }
      let #(next_state, commands) =
        lifecycle.transition(model_state, lifecycle.InstallTeardown)
      let stored = case current {
        Ok(Entry(phase: _, teardown: teardown)) -> teardown
        Error(_) -> None
      }

      run_commands(
        commands,
        fn() { Nil },
        stored,
        Some(incoming),
        on_diagnostic,
      )

      actor.continue(State(
        entries: update_entry(entries, id, next_state, stored, Some(incoming)),
        on_diagnostic:,
      ))
    }

    Notify(id, kind, work) ->
      case dict.get(entries, id) {
        Error(_) -> actor.continue(state)
        Ok(entry) -> {
          let Entry(phase: _, teardown: stored) = entry
          let #(next_state, commands) =
            lifecycle.transition(lifecycle_state(entry), lifecycle.Notify(kind))

          run_commands(commands, work, stored, None, on_diagnostic)

          actor.continue(State(
            entries: update_entry(entries, id, next_state, stored, None),
            on_diagnostic:,
          ))
        }
      }

    Cancel(id) ->
      case dict.get(entries, id) {
        Error(_) -> actor.continue(state)
        Ok(entry) -> {
          let Entry(phase: _, teardown: stored) = entry
          let #(next_state, commands) =
            lifecycle.transition(lifecycle_state(entry), lifecycle.Cancel)

          run_commands(commands, fn() { Nil }, stored, None, on_diagnostic)

          actor.continue(State(
            entries: update_entry(entries, id, next_state, stored, None),
            on_diagnostic:,
          ))
        }
      }

    Stop -> actor.stop()
  }
}

fn lifecycle_state(entry: Entry) -> lifecycle.State {
  let Entry(phase:, teardown:) = entry
  lifecycle.Active(
    phase: phase,
    teardown_ready: case teardown {
      Some(_) -> True
      None -> False
    },
  )
}

fn update_entry(
  entries: Dict(reference.Reference, Entry),
  id: reference.Reference,
  next_state: lifecycle.State,
  stored: Option(fn() -> Nil),
  incoming: Option(fn() -> Nil),
) -> Dict(reference.Reference, Entry) {
  case next_state {
    lifecycle.Closed -> dict.delete(entries, id)
    lifecycle.Active(phase:, teardown_ready:) -> {
      let teardown = case teardown_ready, stored, incoming {
        False, _, _ -> None
        True, Some(existing), _ -> Some(existing)
        True, None, Some(new_teardown) -> Some(new_teardown)
        True, None, None -> None
      }
      dict.insert(entries, id, Entry(phase:, teardown:))
    }
  }
}

fn run_commands(
  commands: List(lifecycle.Command),
  deliver: fn() -> Nil,
  stored: Option(fn() -> Nil),
  incoming: Option(fn() -> Nil),
  on_diagnostic: fn(RuntimeDiagnostic) -> Nil,
) -> Nil {
  case commands {
    [] -> Nil
    [command, ..rest] -> {
      case command {
        lifecycle.Deliver -> deliver()
        lifecycle.RunStoredTeardown -> run_optional(stored)
        lifecycle.RunIncomingTeardown -> run_optional(incoming)
        lifecycle.ReportProtocolError(reason) ->
          on_diagnostic(ProtocolViolation(reason))
        lifecycle.ReportDuplicateTeardown ->
          on_diagnostic(DuplicateTeardownRegistration)
      }
      run_commands(rest, deliver, stored, incoming, on_diagnostic)
    }
  }
}

fn run_optional(callback: Option(fn() -> Nil)) -> Nil {
  case callback {
    Some(run) -> run()
    None -> Nil
  }
}

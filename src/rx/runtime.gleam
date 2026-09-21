import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/erlang/reference
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import rx/lifecycle
import rx/protocol

pub opaque type Runtime {
  Runtime(pid: process.Pid, subject: process.Subject(Message))
}

pub opaque type SubscriptionKey {
  SubscriptionKey(reference.Reference)
}

/// Identity for one asynchronous flattening flow owned by the Runtime actor.
pub opaque type FlowKey {
  FlowKey(reference.Reference)
}

/// Ordering policy for successful asynchronous completions.
pub type FlowOrder {
  /// Emit each result as soon as its asynchronous work completes.
  CompletionOrder
  /// Work may run concurrently, but buffer successful results until all earlier
  /// input sequence numbers have emitted.
  InputOrder
}

pub type FlowCompletion {
  FlowSuccess
  FlowFailure
}

pub type RuntimeError {
  RuntimeStopped
  InvalidConcurrency
}

pub type RuntimeDiagnostic {
  ProtocolViolation(protocol.ProtocolError)
  DuplicateTeardownRegistration
}

type Entry {
  Entry(phase: protocol.Phase, teardown: Option(fn() -> Nil))
}

type Fifo(item) {
  Fifo(front: List(item), back: List(item))
}

type PendingWork {
  PendingWork(sequence: Int, start: fn(Int) -> fn() -> Nil)
}

type ActiveWork {
  ActiveWork(sequence: Int, cancel: fn() -> Nil)
}

type CompletedWork {
  CompletedWork(sequence: Int, deliver: fn() -> Nil)
}

type FlowEntry {
  FlowEntry(
    concurrency: Int,
    order: FlowOrder,
    next_sequence: Int,
    next_emit: Int,
    pending: Fifo(PendingWork),
    active: List(ActiveWork),
    active_count: Int,
    completed: List(CompletedWork),
    input_done: Bool,
    on_drain: fn() -> Nil,
  )
}

type State {
  State(
    entries: Dict(reference.Reference, Entry),
    flows: Dict(reference.Reference, FlowEntry),
    on_diagnostic: fn(RuntimeDiagnostic) -> Nil,
  )
}

type Message {
  Register(reference.Reference)
  SetTeardown(reference.Reference, fn() -> Nil)
  Notify(reference.Reference, protocol.Kind, fn() -> Nil)
  Cancel(reference.Reference)
  RegisterFlow(reference.Reference, Int, FlowOrder, fn() -> Nil)
  EnqueueFlow(reference.Reference, fn(Int) -> fn() -> Nil)
  CompleteFlow(reference.Reference, Int, FlowCompletion, fn() -> Nil)
  FinishFlowInput(reference.Reference)
  FailFlowInput(reference.Reference, fn() -> Nil)
  CancelFlow(reference.Reference)
  Stop
}

pub fn start() -> Result(Runtime, actor.StartError) {
  start_checked(fn(_) { Nil })
}

pub fn start_checked(
  on_diagnostic: fn(RuntimeDiagnostic) -> Nil,
) -> Result(Runtime, actor.StartError) {
  actor.new(State(entries: dict.new(), flows: dict.new(), on_diagnostic:))
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) {
    let actor.Started(pid:, data: subject) = started
    Runtime(pid:, subject:)
  })
}

/// Reserve one subscription entry without blocking on the runtime actor.
///
/// Registration is deliberately one-way so observer callbacks can subscribe to
/// additional streams on the same runtime without self-deadlocking. The
/// `Register` message is sent before producer code can emit or spawn work, so
/// subsequent messages from that subscription are ordered behind registration.
pub fn register(runtime: Runtime) -> Result(SubscriptionKey, RuntimeError) {
  let Runtime(pid:, subject:) = runtime
  case process.is_alive(pid) {
    False -> Error(RuntimeStopped)
    True -> {
      let id = reference.new()
      process.send(subject, Register(id))
      Ok(SubscriptionKey(id))
    }
  }
}

pub fn set_teardown(
  runtime: Runtime,
  key: SubscriptionKey,
  teardown: fn() -> Nil,
) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let SubscriptionKey(id) = key
  process.send(subject, SetTeardown(id, teardown))
}

pub fn dispatch(
  runtime: Runtime,
  key: SubscriptionKey,
  kind: protocol.Kind,
  work: fn() -> Nil,
) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let SubscriptionKey(id) = key
  process.send(subject, Notify(id, kind, work))
}

pub fn cancel(runtime: Runtime, key: SubscriptionKey) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let SubscriptionKey(id) = key
  process.send(subject, Cancel(id))
}

/// Register serialized state for an async flattening operator.
///
/// This does not create another actor. Queue state is stored inside the existing
/// Runtime actor. `on_drain` runs after the input has completed and all accepted
/// work has completed and emitted.
///
/// Like subscription registration, flow registration is one-way and reentrant:
/// callbacks running on the runtime actor may construct and subscribe nested
/// flows without waiting for the actor to reply to itself.
pub fn register_flow(
  runtime: Runtime,
  concurrency: Int,
  order: FlowOrder,
  on_drain: fn() -> Nil,
) -> Result(FlowKey, RuntimeError) {
  case concurrency > 0 {
    False -> Error(InvalidConcurrency)
    True -> {
      let Runtime(pid:, subject:) = runtime
      case process.is_alive(pid) {
        False -> Error(RuntimeStopped)
        True -> {
          let id = reference.new()
          process.send(subject, RegisterFlow(id, concurrency, order, on_drain))
          Ok(FlowKey(id))
        }
      }
    }
  }
}

/// Add one unit of work to a flow in FIFO input order.
///
/// The runtime assigns a monotonically increasing sequence number. `start` is
/// invoked only when a concurrency slot is available and returns the physical
/// cancellation callback for that unit of work.
pub fn enqueue_flow(
  runtime: Runtime,
  key: FlowKey,
  start: fn(Int) -> fn() -> Nil,
) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let FlowKey(id) = key
  process.send(subject, EnqueueFlow(id, start))
}

/// Report completion of an asynchronously running flow item.
///
/// `deliver` is a zero-argument typed closure capturing the successful value or
/// error. This lets the runtime buffer/reorder work without erasing values to
/// `Dynamic`.
pub fn complete_flow(
  runtime: Runtime,
  key: FlowKey,
  sequence: Int,
  completion: FlowCompletion,
  deliver: fn() -> Nil,
) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let FlowKey(id) = key
  process.send(subject, CompleteFlow(id, sequence, completion, deliver))
}

pub fn finish_flow_input(runtime: Runtime, key: FlowKey) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let FlowKey(id) = key
  process.send(subject, FinishFlowInput(id))
}

/// Fail the upstream input itself and cancel all active projected work.
pub fn fail_flow_input(
  runtime: Runtime,
  key: FlowKey,
  deliver_error: fn() -> Nil,
) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let FlowKey(id) = key
  process.send(subject, FailFlowInput(id, deliver_error))
}

pub fn cancel_flow(runtime: Runtime, key: FlowKey) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  let FlowKey(id) = key
  process.send(subject, CancelFlow(id))
}

/// Stop the runtime after cancelling every active subscription and flow.
///
/// This call is intentionally asynchronous so it is safe to invoke from an
/// observer callback running on the runtime actor itself. Active source
/// teardowns and Future cancellation callbacks run before the actor exits.
pub fn stop(runtime: Runtime) -> Nil {
  let Runtime(pid: _, subject:) = runtime
  process.send(subject, Stop)
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    Register(id) ->
      actor.continue(
        State(
          ..state,
          entries: dict.insert(
            state.entries,
            id,
            Entry(phase: protocol.Open, teardown: None),
          ),
        ),
      )

    SetTeardown(id, incoming) -> {
      let current = dict.get(state.entries, id)
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
        state.on_diagnostic,
      )

      actor.continue(
        State(
          ..state,
          entries: update_entry(
            state.entries,
            id,
            next_state,
            stored,
            Some(incoming),
          ),
        ),
      )
    }

    Notify(id, kind, work) ->
      case dict.get(state.entries, id) {
        Error(_) -> actor.continue(state)
        Ok(entry) -> {
          let Entry(phase: _, teardown: stored) = entry
          let #(next_state, commands) =
            lifecycle.transition(lifecycle_state(entry), lifecycle.Notify(kind))

          run_commands(commands, work, stored, None, state.on_diagnostic)

          actor.continue(
            State(
              ..state,
              entries: update_entry(state.entries, id, next_state, stored, None),
            ),
          )
        }
      }

    Cancel(id) ->
      case dict.get(state.entries, id) {
        Error(_) -> actor.continue(state)
        Ok(entry) -> {
          let Entry(phase: _, teardown: stored) = entry
          let #(next_state, commands) =
            lifecycle.transition(lifecycle_state(entry), lifecycle.Cancel)

          run_commands(
            commands,
            fn() { Nil },
            stored,
            None,
            state.on_diagnostic,
          )

          actor.continue(
            State(
              ..state,
              entries: update_entry(state.entries, id, next_state, stored, None),
            ),
          )
        }
      }

    RegisterFlow(id, concurrency, order, on_drain) ->
      actor.continue(
        State(
          ..state,
          flows: dict.insert(
            state.flows,
            id,
            FlowEntry(
              concurrency:,
              order:,
              next_sequence: 0,
              next_emit: 0,
              pending: fifo_new(),
              active: [],
              active_count: 0,
              completed: [],
              input_done: False,
              on_drain:,
            ),
          ),
        ),
      )

    EnqueueFlow(id, start_work) ->
      case dict.get(state.flows, id) {
        Error(_) -> actor.continue(state)
        Ok(flow) -> {
          let sequence = flow.next_sequence
          let queued =
            FlowEntry(
              ..flow,
              next_sequence: sequence + 1,
              pending: fifo_push(
                flow.pending,
                PendingWork(sequence:, start: start_work),
              ),
            )
          let running = fill_slots(queued)
          actor.continue(
            State(..state, flows: dict.insert(state.flows, id, running)),
          )
        }
      }

    CompleteFlow(id, sequence, completion, deliver) ->
      case dict.get(state.flows, id) {
        Error(_) -> actor.continue(state)
        Ok(flow) ->
          case remove_active(flow.active, sequence) {
            #(False, _) -> actor.continue(state)
            #(True, remaining_active) -> {
              let without_active =
                FlowEntry(
                  ..flow,
                  active: remaining_active,
                  active_count: flow.active_count - 1,
                )

              case completion {
                FlowFailure -> {
                  deliver()
                  cancel_active(remaining_active)
                  actor.continue(
                    State(..state, flows: dict.delete(state.flows, id)),
                  )
                }
                FlowSuccess -> {
                  let emitted = case flow.order {
                    CompletionOrder -> {
                      deliver()
                      without_active
                    }
                    InputOrder ->
                      flush_ordered(
                        FlowEntry(..without_active, completed: [
                          CompletedWork(sequence:, deliver:),
                          ..without_active.completed
                        ]),
                      )
                  }
                  let refilled = fill_slots(emitted)
                  continue_flow(state, id, refilled)
                }
              }
            }
          }
      }

    FinishFlowInput(id) ->
      case dict.get(state.flows, id) {
        Error(_) -> actor.continue(state)
        Ok(flow) ->
          continue_flow(state, id, FlowEntry(..flow, input_done: True))
      }

    FailFlowInput(id, deliver_error) ->
      case dict.get(state.flows, id) {
        Error(_) -> actor.continue(state)
        Ok(flow) -> {
          deliver_error()
          cancel_active(flow.active)
          actor.continue(State(..state, flows: dict.delete(state.flows, id)))
        }
      }

    CancelFlow(id) ->
      case dict.get(state.flows, id) {
        Error(_) -> actor.continue(state)
        Ok(flow) -> {
          cancel_active(flow.active)
          actor.continue(State(..state, flows: dict.delete(state.flows, id)))
        }
      }

    Stop -> {
      cancel_all_entries(state.entries)
      cancel_all_flows(state.flows)
      actor.stop()
    }
  }
}

fn continue_flow(
  state: State,
  id: reference.Reference,
  flow: FlowEntry,
) -> actor.Next(State, Message) {
  case flow_is_drained(flow) {
    True -> {
      flow.on_drain()
      actor.continue(State(..state, flows: dict.delete(state.flows, id)))
    }
    False ->
      actor.continue(State(..state, flows: dict.insert(state.flows, id, flow)))
  }
}

fn fill_slots(flow: FlowEntry) -> FlowEntry {
  case flow.active_count < flow.concurrency {
    False -> flow
    True ->
      case fifo_pop(flow.pending) {
        Error(_) -> flow
        Ok(#(PendingWork(sequence:, start: start_work), remaining)) -> {
          let cancel_work = start_work(sequence)
          fill_slots(
            FlowEntry(
              ..flow,
              pending: remaining,
              active: [
                ActiveWork(sequence:, cancel: cancel_work),
                ..flow.active
              ],
              active_count: flow.active_count + 1,
            ),
          )
        }
      }
  }
}

fn flush_ordered(flow: FlowEntry) -> FlowEntry {
  case take_completed(flow.completed, flow.next_emit) {
    Error(_) -> flow
    Ok(#(deliver, remaining)) -> {
      deliver()
      flush_ordered(
        FlowEntry(..flow, next_emit: flow.next_emit + 1, completed: remaining),
      )
    }
  }
}

fn flow_is_drained(flow: FlowEntry) -> Bool {
  flow.input_done
  && flow.active_count == 0
  && fifo_is_empty(flow.pending)
  && list_is_empty(flow.completed)
}

fn remove_active(
  active: List(ActiveWork),
  sequence: Int,
) -> #(Bool, List(ActiveWork)) {
  case active {
    [] -> #(False, [])
    [item, ..rest] ->
      case item.sequence == sequence {
        True -> #(True, rest)
        False -> {
          let #(found, remaining) = remove_active(rest, sequence)
          #(found, [item, ..remaining])
        }
      }
  }
}

fn take_completed(
  completed: List(CompletedWork),
  sequence: Int,
) -> Result(#(fn() -> Nil, List(CompletedWork)), Nil) {
  case completed {
    [] -> Error(Nil)
    [item, ..rest] ->
      case item.sequence == sequence {
        True -> Ok(#(item.deliver, rest))
        False ->
          case take_completed(rest, sequence) {
            Error(_) -> Error(Nil)
            Ok(#(deliver, remaining)) -> Ok(#(deliver, [item, ..remaining]))
          }
      }
  }
}

fn cancel_active(active: List(ActiveWork)) -> Nil {
  case active {
    [] -> Nil
    [first, ..rest] -> {
      first.cancel()
      cancel_active(rest)
    }
  }
}

fn cancel_all_entries(entries: Dict(reference.Reference, Entry)) -> Nil {
  dict.each(entries, fn(_, entry) { run_optional(entry.teardown) })
}

fn cancel_all_flows(flows: Dict(reference.Reference, FlowEntry)) -> Nil {
  dict.each(flows, fn(_, flow) { cancel_active(flow.active) })
}

fn fifo_new() -> Fifo(item) {
  Fifo(front: [], back: [])
}

fn fifo_push(fifo: Fifo(item), item: item) -> Fifo(item) {
  Fifo(..fifo, back: [item, ..fifo.back])
}

fn fifo_pop(fifo: Fifo(item)) -> Result(#(item, Fifo(item)), Nil) {
  case fifo.front {
    [first, ..rest] -> Ok(#(first, Fifo(..fifo, front: rest)))
    [] ->
      case list.reverse(fifo.back) {
        [] -> Error(Nil)
        [first, ..rest] -> Ok(#(first, Fifo(front: rest, back: [])))
      }
  }
}

fn fifo_is_empty(fifo: Fifo(item)) -> Bool {
  case fifo.front, fifo.back {
    [], [] -> True
    _, _ -> False
  }
}

fn list_is_empty(items: List(item)) -> Bool {
  case items {
    [] -> True
    _ -> False
  }
}

fn lifecycle_state(entry: Entry) -> lifecycle.State {
  let Entry(phase:, teardown:) = entry
  lifecycle.Active(phase: phase, teardown_ready: case teardown {
    Some(_) -> True
    None -> False
  })
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

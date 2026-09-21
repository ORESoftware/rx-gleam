import gleam/list

/// Pure reference model for the asynchronous flattening state machine.
///
/// The production runtime stores typed start/deliver/cancel closures. This model
/// intentionally stores only sequence numbers, making it deterministic and easy
/// to exhaustively explore without BEAM timing or callback effects.
pub type Order {
  CompletionOrder
  InputOrder
}

pub type Status {
  Running
  Failed
  Cancelled
  Drained
}

pub type State {
  State(
    concurrency: Int,
    order: Order,
    next_sequence: Int,
    next_emit: Int,
    pending: List(Int),
    active: List(Int),
    completed: List(Int),
    input_done: Bool,
    status: Status,
  )
}

pub type Event {
  Enqueue
  CompleteSuccess(Int)
  CompleteFailure(Int)
  FinishInput
  Cancel
}

pub type Command {
  Start(Int)
  Emit(Int)
  CancelActive(Int)
  Fail
  Drain
}

pub fn new(concurrency: Int, order: Order) -> State {
  State(
    concurrency: concurrency,
    order: order,
    next_sequence: 0,
    next_emit: 0,
    pending: [],
    active: [],
    completed: [],
    input_done: False,
    status: Running,
  )
}

pub fn transition(state: State, event: Event) -> #(State, List(Command)) {
  case state.status {
    Failed -> #(state, [])
    Cancelled -> #(state, [])
    Drained -> #(state, [])
    Running -> transition_running(state, event)
  }
}

pub fn invariants_hold(state: State) -> Bool {
  let capacity_ok =
    state.concurrency > 0 && list.length(state.active) <= state.concurrency
  let sequence_bounds =
    state.next_sequence >= 0
    && state.next_emit >= 0
    && state.next_emit <= state.next_sequence
  let unique_sets =
    unique(state.pending) && unique(state.active) && unique(state.completed)
  let disjoint_sets =
    disjoint(state.pending, state.active)
    && disjoint(state.pending, state.completed)
    && disjoint(state.active, state.completed)
  let known_sequences =
    all_below(state.pending, state.next_sequence)
    && all_below(state.active, state.next_sequence)
    && all_below(state.completed, state.next_sequence)
  let terminal_is_empty = case state.status {
    Running -> True
    Failed -> empty_work(state)
    Cancelled -> empty_work(state)
    Drained -> empty_work(state) && state.input_done
  }

  capacity_ok
  && sequence_bounds
  && unique_sets
  && disjoint_sets
  && known_sequences
  && terminal_is_empty
}

fn transition_running(state: State, event: Event) -> #(State, List(Command)) {
  case event {
    Enqueue ->
      case state.input_done {
        True -> #(state, [])
        False -> {
          let sequence = state.next_sequence
          let queued =
            State(
              ..state,
              next_sequence: sequence + 1,
              pending: list.append(state.pending, [sequence]),
            )
          fill_slots(queued, [])
        }
      }

    CompleteSuccess(sequence) ->
      case remove(state.active, sequence) {
        #(False, _) -> #(state, [])
        #(True, remaining_active) -> {
          let inactive = State(..state, active: remaining_active)
          let #(emitted, emit_commands) = case state.order {
            CompletionOrder -> #(inactive, [Emit(sequence)])
            InputOrder ->
              flush_ordered(
                State(..inactive, completed: [sequence, ..inactive.completed]),
              )
          }
          let #(refilled, start_commands) = fill_slots(emitted, [])
          finish_if_drained(
            refilled,
            list.append(emit_commands, start_commands),
          )
        }
      }

    CompleteFailure(sequence) ->
      case remove(state.active, sequence) {
        #(False, _) -> #(state, [])
        #(True, remaining_active) -> {
          let cancel_commands = cancel_commands(remaining_active)
          #(
            State(
              ..state,
              pending: [],
              active: [],
              completed: [],
              status: Failed,
            ),
            [Fail, ..cancel_commands],
          )
        }
      }

    FinishInput -> finish_if_drained(State(..state, input_done: True), [])

    Cancel -> #(
      State(..state, pending: [], active: [], completed: [], status: Cancelled),
      cancel_commands(state.active),
    )
  }
}

fn fill_slots(
  state: State,
  commands: List(Command),
) -> #(State, List(Command)) {
  case list.length(state.active) < state.concurrency, state.pending {
    True, [next, ..rest] ->
      fill_slots(
        State(..state, pending: rest, active: [next, ..state.active]),
        list.append(commands, [Start(next)]),
      )
    _, _ -> #(state, commands)
  }
}

fn flush_ordered(state: State) -> #(State, List(Command)) {
  case remove(state.completed, state.next_emit) {
    #(False, _) -> #(state, [])
    #(True, remaining) -> {
      let sequence = state.next_emit
      let #(next, later_commands) =
        flush_ordered(
          State(..state, next_emit: sequence + 1, completed: remaining),
        )
      #(next, [Emit(sequence), ..later_commands])
    }
  }
}

fn finish_if_drained(
  state: State,
  commands: List(Command),
) -> #(State, List(Command)) {
  case state.input_done && empty_work(state) {
    True -> #(State(..state, status: Drained), list.append(commands, [Drain]))
    False -> #(state, commands)
  }
}

fn empty_work(state: State) -> Bool {
  state.pending == [] && state.active == [] && state.completed == []
}

fn remove(items: List(Int), wanted: Int) -> #(Bool, List(Int)) {
  case items {
    [] -> #(False, [])
    [first, ..rest] ->
      case first == wanted {
        True -> #(True, rest)
        False -> {
          let #(found, remaining) = remove(rest, wanted)
          #(found, [first, ..remaining])
        }
      }
  }
}

fn cancel_commands(active: List(Int)) -> List(Command) {
  case active {
    [] -> []
    [first, ..rest] -> [CancelActive(first), ..cancel_commands(rest)]
  }
}

fn unique(items: List(Int)) -> Bool {
  case items {
    [] -> True
    [first, ..rest] -> !contains(rest, first) && unique(rest)
  }
}

fn disjoint(left: List(Int), right: List(Int)) -> Bool {
  case left {
    [] -> True
    [first, ..rest] -> !contains(right, first) && disjoint(rest, right)
  }
}

fn all_below(items: List(Int), bound: Int) -> Bool {
  case items {
    [] -> True
    [first, ..rest] -> first >= 0 && first < bound && all_below(rest, bound)
  }
}

fn contains(items: List(Int), wanted: Int) -> Bool {
  case items {
    [] -> False
    [first, ..rest] -> first == wanted || contains(rest, wanted)
  }
}

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
    emitted: List(Int),
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
    emitted: [],
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
    unique(state.pending)
    && unique(state.active)
    && unique(state.completed)
    && unique(state.emitted)
  let disjoint_sets =
    disjoint(state.pending, state.active)
    && disjoint(state.pending, state.completed)
    && disjoint(state.pending, state.emitted)
    && disjoint(state.active, state.completed)
    && disjoint(state.active, state.emitted)
    && disjoint(state.completed, state.emitted)
  let known_sequences =
    all_below(state.pending, state.next_sequence)
    && all_below(state.active, state.next_sequence)
    && all_below(state.completed, state.next_sequence)
    && all_below(state.emitted, state.next_sequence)
  let running_conserves_work = case state.status {
    Running -> accepted_exactly_once(state, 0)
    _ -> True
  }
  let ordering_consistent = case state.order {
    InputOrder ->
      state.emitted == prefix(state.next_emit)
      && state.next_emit == list.length(state.emitted)
    CompletionOrder -> state.completed == []
  }
  let terminal_is_empty = case state.status {
    Running -> True
    Failed -> empty_work(state)
    Cancelled -> empty_work(state)
    Drained ->
      empty_work(state)
      && state.input_done
      && list.length(state.emitted) == state.next_sequence
  }

  capacity_ok
  && sequence_bounds
  && unique_sets
  && disjoint_sets
  && known_sequences
  && running_conserves_work
  && ordering_consistent
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
          let #(emitted_state, emit_commands) = case state.order {
            CompletionOrder -> #(
              State(
                ..inactive,
                emitted: list.append(inactive.emitted, [sequence]),
              ),
              [Emit(sequence)],
            )
            InputOrder ->
              flush_ordered(
                State(..inactive, completed: [sequence, ..inactive.completed]),
              )
          }
          let #(refilled, start_commands) = fill_slots(emitted_state, [])
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
          State(
            ..state,
            next_emit: sequence + 1,
            completed: remaining,
            emitted: list.append(state.emitted, [sequence]),
          ),
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

fn accepted_exactly_once(state: State, sequence: Int) -> Bool {
  case sequence >= state.next_sequence {
    True -> True
    False ->
      count_occurrences(state.pending, sequence)
      + count_occurrences(state.active, sequence)
      + count_occurrences(state.completed, sequence)
      + count_occurrences(state.emitted, sequence)
      == 1
      && accepted_exactly_once(state, sequence + 1)
  }
}

fn count_occurrences(items: List(Int), wanted: Int) -> Int {
  case items {
    [] -> 0
    [first, ..rest] ->
      case first == wanted {
        True -> 1 + count_occurrences(rest, wanted)
        False -> count_occurrences(rest, wanted)
      }
  }
}

fn prefix(length: Int) -> List(Int) {
  prefix_loop(0, length, []) |> list.reverse
}

fn prefix_loop(current: Int, length: Int, reversed: List(Int)) -> List(Int) {
  case current >= length {
    True -> reversed
    False -> prefix_loop(current + 1, length, [current, ..reversed])
  }
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

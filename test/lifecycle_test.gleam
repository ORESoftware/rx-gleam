import gleam/list
import gleeunit
import gleeunit/should
import rx/lifecycle
import rx/protocol

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn exhaustive_lifecycle_sequences_up_to_length_six_test() {
  all_sequences(6)
  |> assert_all_safe
}

pub fn terminal_with_teardown_closes_and_releases_test() {
  let #(installed, install_commands) =
    lifecycle.transition(lifecycle.initial(), lifecycle.InstallTeardown)
  install_commands |> should.equal([])

  let #(closed, commands) =
    lifecycle.transition(installed, lifecycle.Notify(protocol.CompleteKind))

  closed |> should.equal(lifecycle.Closed)
  commands |> should.equal([lifecycle.Deliver, lifecycle.RunStoredTeardown])
}

pub fn terminal_before_teardown_releases_when_teardown_arrives_test() {
  let #(terminated, commands) =
    lifecycle.transition(
      lifecycle.initial(),
      lifecycle.Notify(protocol.CompleteKind),
    )
  commands |> should.equal([lifecycle.Deliver])

  let #(closed, release_commands) =
    lifecycle.transition(terminated, lifecycle.InstallTeardown)
  closed |> should.equal(lifecycle.Closed)
  release_commands |> should.equal([lifecycle.RunIncomingTeardown])
}

fn alphabet() -> List(lifecycle.Action) {
  [
    lifecycle.InstallTeardown,
    lifecycle.Notify(protocol.NextKind),
    lifecycle.Notify(protocol.ErrorKind),
    lifecycle.Notify(protocol.CompleteKind),
    lifecycle.Cancel,
  ]
}

fn all_sequences(max_length: Int) -> List(List(lifecycle.Action)) {
  generate([[]], max_length)
}

fn generate(
  current: List(List(lifecycle.Action)),
  remaining: Int,
) -> List(List(lifecycle.Action)) {
  case remaining {
    0 -> current
    _ -> {
      let extended = prepend_each(current, alphabet())
      generate(list.append(current, extended), remaining - 1)
    }
  }
}

fn prepend_each(
  sequences: List(List(lifecycle.Action)),
  symbols: List(lifecycle.Action),
) -> List(List(lifecycle.Action)) {
  case sequences {
    [] -> []
    [sequence, ..rest] ->
      list.append(attach(sequence, symbols), prepend_each(rest, symbols))
  }
}

fn attach(
  sequence: List(lifecycle.Action),
  symbols: List(lifecycle.Action),
) -> List(List(lifecycle.Action)) {
  case symbols {
    [] -> []
    [symbol, ..rest] -> [[symbol, ..sequence], ..attach(sequence, rest)]
  }
}

fn assert_all_safe(sequences: List(List(lifecycle.Action))) -> Nil {
  case sequences {
    [] -> Nil
    [sequence, ..rest] -> {
      trace_is_safe(sequence, lifecycle.initial(), 0)
      |> should.equal(True)
      assert_all_safe(rest)
    }
  }
}

fn trace_is_safe(
  actions: List(lifecycle.Action),
  state: lifecycle.State,
  stored_teardown_runs: Int,
) -> Bool {
  case actions {
    [] -> stored_teardown_runs <= 1
    [action, ..rest] -> {
      let #(next_state, commands) = lifecycle.transition(state, action)
      let next_runs = stored_teardown_runs + count_stored_teardowns(commands)
      let absorbing = case state {
        lifecycle.Closed -> next_state == lifecycle.Closed
        lifecycle.Active(..) -> True
      }
      absorbing && next_runs <= 1 && trace_is_safe(rest, next_state, next_runs)
    }
  }
}

fn count_stored_teardowns(commands: List(lifecycle.Command)) -> Int {
  case commands {
    [] -> 0
    [first, ..rest] -> {
      let current = case first {
        lifecycle.RunStoredTeardown -> 1
        _ -> 0
      }
      current + count_stored_teardowns(rest)
    }
  }
}

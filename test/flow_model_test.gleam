import gleeunit
import gleeunit/should
import rx/flow_model as model

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn concat_reference_model_is_fifo_test() {
  let state0 = model.new(1, model.InputOrder)
  let #(state1, commands1) = model.transition(state0, model.Enqueue)
  commands1 |> should.equal([model.Start(0)])

  let #(state2, commands2) = model.transition(state1, model.Enqueue)
  commands2 |> should.equal([])

  let #(state3, commands3) = model.transition(state2, model.Enqueue)
  commands3 |> should.equal([])

  let #(state4, commands4) = model.transition(state3, model.CompleteSuccess(0))
  commands4 |> should.equal([model.Emit(0), model.Start(1)])

  let #(state5, commands5) = model.transition(state4, model.CompleteSuccess(1))
  commands5 |> should.equal([model.Emit(1), model.Start(2)])

  let #(state6, _) = model.transition(state5, model.FinishInput)
  let #(state7, commands7) = model.transition(state6, model.CompleteSuccess(2))
  commands7 |> should.equal([model.Emit(2), model.Drain])
  state7.status |> should.equal(model.Drained)
}

pub fn concurrent_completion_order_model_emits_ready_result_test() {
  let state0 = model.new(2, model.CompletionOrder)
  let #(state1, _) = model.transition(state0, model.Enqueue)
  let #(state2, _) = model.transition(state1, model.Enqueue)
  let #(state3, _) = model.transition(state2, model.Enqueue)

  let #(state4, commands) = model.transition(state3, model.CompleteSuccess(1))
  commands |> should.equal([model.Emit(1), model.Start(2)])
  state4.active |> should.equal([2, 0])
}

pub fn concurrent_input_order_model_buffers_early_completion_test() {
  let state0 = model.new(2, model.InputOrder)
  let #(state1, _) = model.transition(state0, model.Enqueue)
  let #(state2, _) = model.transition(state1, model.Enqueue)

  let #(state3, commands3) = model.transition(state2, model.CompleteSuccess(1))
  commands3 |> should.equal([])
  state3.completed |> should.equal([1])

  let #(state4, commands4) = model.transition(state3, model.CompleteSuccess(0))
  commands4 |> should.equal([model.Emit(0), model.Emit(1)])
  state4.completed |> should.equal([])
  state4.next_emit |> should.equal(2)
}

pub fn failure_is_absorbing_and_cancels_other_active_work_test() {
  let state0 = model.new(2, model.CompletionOrder)
  let #(state1, _) = model.transition(state0, model.Enqueue)
  let #(state2, _) = model.transition(state1, model.Enqueue)
  let #(failed, commands) = model.transition(state2, model.CompleteFailure(1))

  commands |> should.equal([model.Fail, model.CancelActive(0)])
  failed.status |> should.equal(model.Failed)
  model.invariants_hold(failed) |> should.equal(True)

  let #(still_failed, late_commands) =
    model.transition(failed, model.CompleteSuccess(0))
  still_failed |> should.equal(failed)
  late_commands |> should.equal([])
}

pub fn exhaustively_preserves_invariants_concurrency_one_test() {
  explore(model.new(1, model.InputOrder), 6)
}

pub fn exhaustively_preserves_invariants_concurrency_two_ordered_test() {
  explore(model.new(2, model.InputOrder), 6)
}

pub fn exhaustively_preserves_invariants_concurrency_two_unordered_test() {
  explore(model.new(2, model.CompletionOrder), 6)
}

fn explore(state: model.State, remaining: Int) -> Nil {
  model.invariants_hold(state) |> should.equal(True)

  case remaining {
    0 -> Nil
    _ -> explore_events(state, alphabet(), remaining)
  }
}

fn explore_events(
  state: model.State,
  events: List(model.Event),
  remaining: Int,
) -> Nil {
  case events {
    [] -> Nil
    [event, ..rest] -> {
      let #(next, _) = model.transition(state, event)
      let _ = explore(next, remaining - 1)
      explore_events(state, rest, remaining)
    }
  }
}

fn alphabet() -> List(model.Event) {
  [
    model.Enqueue,
    model.CompleteSuccess(0),
    model.CompleteSuccess(1),
    model.CompleteFailure(0),
    model.FinishInput,
    model.Cancel,
  ]
}

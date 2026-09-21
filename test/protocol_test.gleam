import gleam/list
import gleeunit
import gleeunit/should
import rx/protocol.{type Notification}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn valid_open_sequences_test() {
  protocol_expect([
    protocol.OnNext(1),
    protocol.OnNext(2),
    protocol.OnNext(3),
  ])
  |> should.equal(Ok(protocol.Open))
}

pub fn valid_complete_sequence_test() {
  protocol_expect([
    protocol.OnNext(1),
    protocol.OnNext(2),
    protocol.OnComplete,
  ])
  |> should.equal(Ok(protocol.Terminated))
}

pub fn valid_error_sequence_test() {
  protocol_expect([protocol.OnNext(1), protocol.OnError("boom")])
  |> should.equal(Ok(protocol.Terminated))
}

pub fn next_after_complete_is_rejected_test() {
  protocol_expect([
    protocol.OnNext(1),
    protocol.OnComplete,
    protocol.OnNext(2),
  ])
  |> should.equal(Error(protocol.NotificationAfterTermination))
}

pub fn complete_after_error_is_rejected_test() {
  protocol_expect([protocol.OnError("boom"), protocol.OnComplete])
  |> should.equal(Error(protocol.DuplicateTermination))
}

pub fn exhaustive_sequences_up_to_length_six_test() {
  all_sequences(6)
  |> assert_model_agreement
}

fn protocol_expect(sequence: List(Notification(Int, String))) {
  protocol.validate(sequence)
}

fn alphabet() -> List(Notification(Int, String)) {
  [protocol.OnNext(1), protocol.OnError("error"), protocol.OnComplete]
}

fn all_sequences(max_length: Int) -> List(List(Notification(Int, String))) {
  generate([[]], max_length)
}

fn generate(
  current: List(List(Notification(Int, String))),
  remaining: Int,
) -> List(List(Notification(Int, String))) {
  case remaining {
    0 -> current
    _ -> {
      let extended = prepend_each(current, alphabet())
      generate(list.append(current, extended), remaining - 1)
    }
  }
}

fn prepend_each(
  sequences: List(List(Notification(Int, String))),
  symbols: List(Notification(Int, String)),
) -> List(List(Notification(Int, String))) {
  case sequences {
    [] -> []
    [sequence, ..rest] ->
      list.append(attach(sequence, symbols), prepend_each(rest, symbols))
  }
}

fn attach(
  sequence: List(Notification(Int, String)),
  symbols: List(Notification(Int, String)),
) -> List(List(Notification(Int, String))) {
  case symbols {
    [] -> []
    [symbol, ..rest] -> [[symbol, ..sequence], ..attach(sequence, rest)]
  }
}

fn assert_model_agreement(
  sequences: List(List(Notification(Int, String))),
) -> Nil {
  case sequences {
    [] -> Nil
    [sequence, ..rest] -> {
      protocol.validate(sequence)
      |> should.equal(reference_validate(sequence))
      assert_model_agreement(rest)
    }
  }
}

fn reference_validate(
  sequence: List(Notification(Int, String)),
) -> Result(protocol.Phase, protocol.ProtocolError) {
  reference_from(protocol.Open, sequence)
}

fn reference_from(
  phase: protocol.Phase,
  sequence: List(Notification(Int, String)),
) -> Result(protocol.Phase, protocol.ProtocolError) {
  case sequence {
    [] -> Ok(phase)
    [item, ..rest] ->
      case phase {
        protocol.Open ->
          case item {
            protocol.OnNext(_) -> reference_from(protocol.Open, rest)
            protocol.OnError(_) -> reference_from(protocol.Terminated, rest)
            protocol.OnComplete -> reference_from(protocol.Terminated, rest)
          }
        protocol.Terminated ->
          case item {
            protocol.OnNext(_) -> Error(protocol.NotificationAfterTermination)
            protocol.OnError(_) -> Error(protocol.DuplicateTermination)
            protocol.OnComplete -> Error(protocol.DuplicateTermination)
          }
      }
  }
}

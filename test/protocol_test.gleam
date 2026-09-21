import gleeunit
import gleeunit/should
import rx/protocol.{type Notification, Complete, Error, Next}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn valid_open_sequences_test() {
  protocol_expect([Next(1), Next(2), Next(3)])
  |> should.equal(Ok(protocol.Open))
}

pub fn valid_complete_sequence_test() {
  protocol_expect([Next(1), Next(2), Complete])
  |> should.equal(Ok(protocol.Terminated))
}

pub fn valid_error_sequence_test() {
  protocol_expect([Next(1), Error("boom")])
  |> should.equal(Ok(protocol.Terminated))
}

pub fn next_after_complete_is_rejected_test() {
  protocol_expect([Next(1), Complete, Next(2)])
  |> should.equal(Error(protocol.NotificationAfterTermination))
}

pub fn complete_after_error_is_rejected_test() {
  protocol_expect([Error("boom"), Complete])
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
  [Next(1), Error("error"), Complete]
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
      generate(current ++ extended, remaining - 1)
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
      attach(sequence, symbols) ++ prepend_each(rest, symbols)
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
            Next(_) -> reference_from(protocol.Open, rest)
            Error(_) -> reference_from(protocol.Terminated, rest)
            Complete -> reference_from(protocol.Terminated, rest)
          }
        protocol.Terminated ->
          case item {
            Next(_) -> Error(protocol.NotificationAfterTermination)
            Error(_) -> Error(protocol.DuplicateTermination)
            Complete -> Error(protocol.DuplicateTermination)
          }
      }
  }
}

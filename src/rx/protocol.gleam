pub type Notification(value, error) {
  Next(value)
  Error(error)
  Complete
}

pub type Phase {
  Open
  Terminated
}

pub type ProtocolError {
  NotificationAfterTermination
  DuplicateTermination
}

pub fn transition(
  phase: Phase,
  notification: Notification(value, error),
) -> Result(Phase, ProtocolError) {
  case phase, notification {
    Open, Next(_) -> Ok(Open)
    Open, Error(_) -> Ok(Terminated)
    Open, Complete -> Ok(Terminated)
    Terminated, Next(_) -> Error(NotificationAfterTermination)
    Terminated, Error(_) -> Error(DuplicateTermination)
    Terminated, Complete -> Error(DuplicateTermination)
  }
}

pub fn validate(
  notifications: List(Notification(value, error)),
) -> Result(Phase, ProtocolError) {
  validate_from(Open, notifications)
}

fn validate_from(
  phase: Phase,
  notifications: List(Notification(value, error)),
) -> Result(Phase, ProtocolError) {
  case notifications {
    [] -> Ok(phase)
    [first, ..rest] ->
      case transition(phase, first) {
        Ok(next) -> validate_from(next, rest)
        Error(error) -> Error(error)
      }
  }
}

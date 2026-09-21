pub type Notification(value, error) {
  Next(value)
  Error(error)
  Complete
}

pub type Kind {
  NextKind
  ErrorKind
  CompleteKind
}

pub type Phase {
  Open
  Terminated
}

pub type ProtocolError {
  NotificationAfterTermination
  DuplicateTermination
}

pub fn kind(notification: Notification(value, error)) -> Kind {
  case notification {
    Next(_) -> NextKind
    Error(_) -> ErrorKind
    Complete -> CompleteKind
  }
}

pub fn transition(
  phase: Phase,
  event: Kind,
) -> Result(Phase, ProtocolError) {
  case phase, event {
    Open, NextKind -> Ok(Open)
    Open, ErrorKind -> Ok(Terminated)
    Open, CompleteKind -> Ok(Terminated)
    Terminated, NextKind -> Error(NotificationAfterTermination)
    Terminated, ErrorKind -> Error(DuplicateTermination)
    Terminated, CompleteKind -> Error(DuplicateTermination)
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
      case transition(phase, kind(first)) {
        Ok(next) -> validate_from(next, rest)
        Error(error) -> Error(error)
      }
  }
}

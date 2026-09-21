import rx/protocol

pub type State {
  Active(
    phase: protocol.Phase,
    teardown_ready: Bool,
  )
  Closed
}

pub type Action {
  InstallTeardown
  Notify(protocol.Kind)
  Cancel
}

pub type Command {
  Deliver
  RunStoredTeardown
  RunIncomingTeardown
  ReportProtocolError(protocol.ProtocolError)
  ReportDuplicateTeardown
}

pub fn initial() -> State {
  Active(phase: protocol.Open, teardown_ready: False)
}

pub fn transition(state: State, action: Action) -> #(State, List(Command)) {
  case state, action {
    Closed, InstallTeardown -> #(Closed, [RunIncomingTeardown])
    Closed, Notify(_) -> #(Closed, [])
    Closed, Cancel -> #(Closed, [])

    Active(phase: protocol.Open, teardown_ready: False), InstallTeardown ->
      #(
        Active(phase: protocol.Open, teardown_ready: True),
        [],
      )

    Active(phase: protocol.Terminated, teardown_ready: False), InstallTeardown ->
      #(Closed, [RunIncomingTeardown])

    Active(phase, teardown_ready: True), InstallTeardown ->
      #(
        Active(phase: phase, teardown_ready: True),
        [RunIncomingTeardown, ReportDuplicateTeardown],
      )

    Active(phase, teardown_ready), Notify(kind) ->
      case protocol.transition(phase, kind) {
        Error(reason) ->
          #(
            Active(phase: phase, teardown_ready: teardown_ready),
            [ReportProtocolError(reason)],
          )

        Ok(protocol.Open) ->
          #(
            Active(phase: protocol.Open, teardown_ready: teardown_ready),
            [Deliver],
          )

        Ok(protocol.Terminated) ->
          case teardown_ready {
            True -> #(Closed, [Deliver, RunStoredTeardown])
            False ->
              #(
                Active(
                  phase: protocol.Terminated,
                  teardown_ready: False,
                ),
                [Deliver],
              )
          }
      }

    Active(phase: _, teardown_ready: True), Cancel ->
      #(Closed, [RunStoredTeardown])

    Active(phase: _, teardown_ready: False), Cancel -> #(Closed, [])
  }
}

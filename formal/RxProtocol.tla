----------------------------- MODULE RxProtocol -----------------------------
EXTENDS Naturals, Sequences

CONSTANT MaxEvents

VARIABLES phase, history

Open == "Open"
Terminated == "Terminated"
NextEvent == "Next"
ErrorEvent == "Error"
CompleteEvent == "Complete"
Events == {NextEvent, ErrorEvent, CompleteEvent}

Init == /\ phase = Open
        /\ history = << >>

CanAccept(event) ==
  CASE phase = Open -> TRUE
    [] phase = Terminated -> FALSE

Step(event) ==
  /\ Len(history) < MaxEvents
  /\ CanAccept(event)
  /\ history' = Append(history, event)
  /\ phase' = IF event = NextEvent THEN Open ELSE Terminated

Next == \E event \in Events: Step(event)

TypeInvariant == /\ phase \in {Open, Terminated}
                 /\ history \in Seq(Events)

TerminalIsAbsorbing == phase = Terminated => UNCHANGED <<phase, history>>

NoEventsAfterTerminal ==
  \A i, j \in 1..Len(history):
    (i < j /\ history[i] \in {ErrorEvent, CompleteEvent}) => FALSE

Spec == Init /\ [][Next]_<<phase, history>>

=============================================================================

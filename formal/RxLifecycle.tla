---------------------------- MODULE RxLifecycle ----------------------------
EXTENDS Naturals

VARIABLES mode, storedTeardownRuns, terminalDelivered

vars == <<mode, storedTeardownRuns, terminalDelivered>>

OpenNoTeardown == "OpenNoTeardown"
OpenWithTeardown == "OpenWithTeardown"
TerminatedWaiting == "TerminatedWaiting"
Closed == "Closed"
Modes == {OpenNoTeardown, OpenWithTeardown, TerminatedWaiting, Closed}

Init ==
  /\ mode = OpenNoTeardown
  /\ storedTeardownRuns = 0
  /\ terminalDelivered = FALSE

InstallTeardown ==
  CASE mode = OpenNoTeardown ->
         /\ mode' = OpenWithTeardown
         /\ UNCHANGED <<storedTeardownRuns, terminalDelivered>>
    [] mode = OpenWithTeardown -> UNCHANGED vars
    [] mode = TerminatedWaiting ->
         /\ mode' = Closed
         /\ UNCHANGED <<storedTeardownRuns, terminalDelivered>>
    [] mode = Closed -> UNCHANGED vars

NotifyNext ==
  /\ mode \in Modes
  /\ UNCHANGED vars

NotifyTerminal ==
  CASE mode = OpenNoTeardown ->
         /\ mode' = TerminatedWaiting
         /\ terminalDelivered' = TRUE
         /\ UNCHANGED storedTeardownRuns
    [] mode = OpenWithTeardown ->
         /\ mode' = Closed
         /\ storedTeardownRuns' = 1
         /\ terminalDelivered' = TRUE
    [] mode = TerminatedWaiting -> UNCHANGED vars
    [] mode = Closed -> UNCHANGED vars

Cancel ==
  CASE mode = OpenNoTeardown ->
         /\ mode' = Closed
         /\ UNCHANGED <<storedTeardownRuns, terminalDelivered>>
    [] mode = OpenWithTeardown ->
         /\ mode' = Closed
         /\ storedTeardownRuns' = 1
         /\ UNCHANGED terminalDelivered
    [] mode = TerminatedWaiting ->
         /\ mode' = Closed
         /\ UNCHANGED <<storedTeardownRuns, terminalDelivered>>
    [] mode = Closed -> UNCHANGED vars

Next == InstallTeardown \/ NotifyNext \/ NotifyTerminal \/ Cancel

TypeInvariant ==
  /\ mode \in Modes
  /\ storedTeardownRuns \in 0..1
  /\ terminalDelivered \in BOOLEAN

StoredTeardownAtMostOnce == storedTeardownRuns <= 1

OpenHasNotTerminated ==
  mode \in {OpenNoTeardown, OpenWithTeardown} => ~terminalDelivered

TerminatedWaitingHasTerminal ==
  mode = TerminatedWaiting => terminalDelivered

StoredTeardownRunOnlyWhenClosed ==
  storedTeardownRuns = 1 => mode = Closed

NoStoredTeardownPendingAfterTerminal ==
  mode = TerminatedWaiting => storedTeardownRuns = 0

Spec == Init /\ [][Next]_vars

=============================================================================

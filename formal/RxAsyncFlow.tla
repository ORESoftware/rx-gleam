---------------------------- MODULE RxAsyncFlow ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Concurrency, MaxItems, Order

VARIABLES status,
          pending,
          active,
          completed,
          emitted,
          nextSequence,
          nextEmit,
          inputDone

vars == <<status, pending, active, completed, emitted,
          nextSequence, nextEmit, inputDone>>

Running == "Running"
Failed == "Failed"
Cancelled == "Cancelled"
Drained == "Drained"
InputOrder == "InputOrder"
CompletionOrder == "CompletionOrder"
Terminal == {Failed, Cancelled, Drained}
Items == 0..(MaxItems - 1)

Init ==
  /\ status = Running
  /\ pending = << >>
  /\ active = {}
  /\ completed = {}
  /\ emitted = << >>
  /\ nextSequence = 0
  /\ nextEmit = 0
  /\ inputDone = FALSE

Enqueue ==
  /\ status = Running
  /\ ~inputDone
  /\ nextSequence < MaxItems
  /\ pending' = Append(pending, nextSequence)
  /\ nextSequence' = nextSequence + 1
  /\ UNCHANGED <<status, active, completed, emitted, nextEmit, inputDone>>

Start ==
  /\ status = Running
  /\ pending # << >>
  /\ Cardinality(active) < Concurrency
  /\ active' = active \cup {Head(pending)}
  /\ pending' = Tail(pending)
  /\ UNCHANGED <<status, completed, emitted, nextSequence, nextEmit, inputDone>>

CompleteSuccess(item) ==
  /\ status = Running
  /\ item \in active
  /\ active' = active \ {item}
  /\ IF Order = CompletionOrder
        THEN /\ emitted' = Append(emitted, item)
             /\ completed' = completed
        ELSE /\ Order = InputOrder
             /\ completed' = completed \cup {item}
             /\ emitted' = emitted
  /\ UNCHANGED <<status, pending, nextSequence, nextEmit, inputDone>>

FlushOrdered ==
  /\ status = Running
  /\ Order = InputOrder
  /\ nextEmit \in completed
  /\ completed' = completed \ {nextEmit}
  /\ emitted' = Append(emitted, nextEmit)
  /\ nextEmit' = nextEmit + 1
  /\ UNCHANGED <<status, pending, active, nextSequence, inputDone>>

CompleteFailure(item) ==
  /\ status = Running
  /\ item \in active
  /\ status' = Failed
  /\ pending' = << >>
  /\ active' = {}
  /\ completed' = {}
  /\ UNCHANGED <<emitted, nextSequence, nextEmit, inputDone>>

FailInput ==
  /\ status = Running
  /\ status' = Failed
  /\ pending' = << >>
  /\ active' = {}
  /\ completed' = {}
  /\ UNCHANGED <<emitted, nextSequence, nextEmit, inputDone>>

FinishInput ==
  /\ status = Running
  /\ ~inputDone
  /\ inputDone' = TRUE
  /\ UNCHANGED <<status, pending, active, completed, emitted,
                 nextSequence, nextEmit>>

Cancel ==
  /\ status = Running
  /\ status' = Cancelled
  /\ pending' = << >>
  /\ active' = {}
  /\ completed' = {}
  /\ UNCHANGED <<emitted, nextSequence, nextEmit, inputDone>>

Drain ==
  /\ status = Running
  /\ inputDone
  /\ pending = << >>
  /\ active = {}
  /\ completed = {}
  /\ status' = Drained
  /\ UNCHANGED <<pending, active, completed, emitted,
                 nextSequence, nextEmit, inputDone>>

Next ==
  \/ Enqueue
  \/ Start
  \/ (\E item \in Items: CompleteSuccess(item))
  \/ FlushOrdered
  \/ (\E item \in Items: CompleteFailure(item))
  \/ FailInput
  \/ FinishInput
  \/ Cancel
  \/ Drain

NoDuplicates(seq) ==
  \A i, j \in 1..Len(seq): i # j => seq[i] # seq[j]

AllBelow(seq, bound) ==
  \A i \in 1..Len(seq): seq[i] < bound

OrderedPrefix == [i \in 1..nextEmit |-> i - 1]

TypeInvariant ==
  /\ status \in {Running, Failed, Cancelled, Drained}
  /\ Order \in {InputOrder, CompletionOrder}
  /\ Concurrency \in Nat \ {0}
  /\ nextSequence \in 0..MaxItems
  /\ nextEmit \in 0..nextSequence
  /\ pending \in Seq(Items)
  /\ active \subseteq Items
  /\ completed \subseteq Items
  /\ emitted \in Seq(Items)
  /\ inputDone \in BOOLEAN

CapacityBound == Cardinality(active) <= Concurrency

UniqueAndDisjoint ==
  /\ NoDuplicates(pending)
  /\ NoDuplicates(emitted)
  /\ (\A i \in 1..Len(pending): pending[i] \notin active)
  /\ (\A i \in 1..Len(pending): pending[i] \notin completed)
  /\ active \cap completed = {}

KnownSequences ==
  /\ AllBelow(pending, nextSequence)
  /\ active \subseteq 0..(nextSequence - 1)
  /\ completed \subseteq 0..(nextSequence - 1)
  /\ AllBelow(emitted, nextSequence)

InputOrderIsPrefix == Order = InputOrder => emitted = OrderedPrefix

TerminalWorkCleared ==
  status \in Terminal =>
    /\ pending = << >>
    /\ active = {}
    /\ completed = {}

Spec == Init /\ [][Next]_vars

=============================================================================

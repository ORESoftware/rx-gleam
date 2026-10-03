-module(rx@flow_model).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/flow_model.gleam").
-export([new/2, transition/2, invariants_hold/1]).
-export_type([order/0, status/0, state/0, event/0, command/0]).

-type order() :: completion_order | input_order.

-type status() :: running | failed | cancelled | drained.

-type state() :: {state,
        integer(),
        order(),
        integer(),
        integer(),
        list(integer()),
        list(integer()),
        list(integer()),
        list(integer()),
        boolean(),
        status()}.

-type event() :: enqueue |
    {complete_success, integer()} |
    {complete_failure, integer()} |
    finish_input |
    cancel.

-type command() :: {start, integer()} |
    {emit, integer()} |
    {cancel_active, integer()} |
    fail |
    drain.

-file("src/rx/flow_model.gleam", 51).
-spec new(integer(), order()) -> state().
new(Concurrency, Order) ->
    {state, Concurrency, Order, 0, 0, [], [], [], [], false, running}.

-file("src/rx/flow_model.gleam", 295).
-spec cancel_commands(list(integer())) -> list(command()).
cancel_commands(Active) ->
    case Active of
        [] ->
            [];

        [First | Rest] ->
            [{cancel_active, First} | cancel_commands(Rest)]
    end.

-file("src/rx/flow_model.gleam", 242).
-spec empty_work(state()) -> boolean().
empty_work(State) ->
    ((erlang:element(6, State) =:= []) andalso (erlang:element(7, State) =:= []))
    andalso (erlang:element(8, State) =:= []).

-file("src/rx/flow_model.gleam", 232).
-spec finish_if_drained(state(), list(command())) -> {state(), list(command())}.
finish_if_drained(State, Commands) ->
    case erlang:element(10, State) andalso empty_work(State) of
        true ->
            {{state,
                    erlang:element(2, State),
                    erlang:element(3, State),
                    erlang:element(4, State),
                    erlang:element(5, State),
                    erlang:element(6, State),
                    erlang:element(7, State),
                    erlang:element(8, State),
                    erlang:element(9, State),
                    erlang:element(10, State),
                    drained},
                lists:append(Commands, [drain])};

        false ->
            {State, Commands}
    end.

-file("src/rx/flow_model.gleam", 281).
-spec remove(list(integer()), integer()) -> {boolean(), list(integer())}.
remove(Items, Wanted) ->
    case Items of
        [] ->
            {false, []};

        [First | Rest] ->
            case First =:= Wanted of
                true ->
                    {true, Rest};

                false ->
                    {Found, Remaining} = remove(Rest, Wanted),
                    {Found, [First | Remaining]}
            end
    end.

-file("src/rx/flow_model.gleam", 199).
-spec fill_slots(state(), list(command())) -> {state(), list(command())}.
fill_slots(State, Commands) ->
    case {erlang:length(erlang:element(7, State)) < erlang:element(2, State),
        erlang:element(6, State)} of
        {true, [Next | Rest]} ->
            fill_slots(
                {state,
                    erlang:element(2, State),
                    erlang:element(3, State),
                    erlang:element(4, State),
                    erlang:element(5, State),
                    Rest,
                    [Next | erlang:element(7, State)],
                    erlang:element(8, State),
                    erlang:element(9, State),
                    erlang:element(10, State),
                    erlang:element(11, State)},
                lists:append(Commands, [{start, Next}])
            );

        {_, _} ->
            {State, Commands}
    end.

-file("src/rx/flow_model.gleam", 213).
-spec flush_ordered(state()) -> {state(), list(command())}.
flush_ordered(State) ->
    case remove(erlang:element(8, State), erlang:element(5, State)) of
        {false, _} ->
            {State, []};

        {true, Remaining} ->
            Sequence = erlang:element(5, State),
            {Next, Later_commands} = flush_ordered(
                {state,
                    erlang:element(2, State),
                    erlang:element(3, State),
                    erlang:element(4, State),
                    Sequence + 1,
                    erlang:element(6, State),
                    erlang:element(7, State),
                    Remaining,
                    lists:append(erlang:element(9, State), [Sequence]),
                    erlang:element(10, State),
                    erlang:element(11, State)}
            ),
            {Next, [{emit, Sequence} | Later_commands]}
    end.

-file("src/rx/flow_model.gleam", 129).
-spec transition_running(state(), event()) -> {state(), list(command())}.
transition_running(State, Event) ->
    case Event of
        enqueue ->
            case erlang:element(10, State) of
                true ->
                    {State, []};

                false ->
                    Sequence = erlang:element(4, State),
                    Queued = {state,
                        erlang:element(2, State),
                        erlang:element(3, State),
                        Sequence + 1,
                        erlang:element(5, State),
                        lists:append(erlang:element(6, State), [Sequence]),
                        erlang:element(7, State),
                        erlang:element(8, State),
                        erlang:element(9, State),
                        erlang:element(10, State),
                        erlang:element(11, State)},
                    fill_slots(Queued, [])
            end;

        {complete_success, Sequence@1} ->
            case remove(erlang:element(7, State), Sequence@1) of
                {false, _} ->
                    {State, []};

                {true, Remaining_active} ->
                    Inactive = {state,
                        erlang:element(2, State),
                        erlang:element(3, State),
                        erlang:element(4, State),
                        erlang:element(5, State),
                        erlang:element(6, State),
                        Remaining_active,
                        erlang:element(8, State),
                        erlang:element(9, State),
                        erlang:element(10, State),
                        erlang:element(11, State)},
                    {Emitted_state, Emit_commands} = case erlang:element(
                        3,
                        State
                    ) of
                        completion_order ->
                            {{state,
                                    erlang:element(2, Inactive),
                                    erlang:element(3, Inactive),
                                    erlang:element(4, Inactive),
                                    erlang:element(5, Inactive),
                                    erlang:element(6, Inactive),
                                    erlang:element(7, Inactive),
                                    erlang:element(8, Inactive),
                                    lists:append(
                                        erlang:element(9, Inactive),
                                        [Sequence@1]
                                    ),
                                    erlang:element(10, Inactive),
                                    erlang:element(11, Inactive)},
                                [{emit, Sequence@1}]};

                        input_order ->
                            flush_ordered(
                                {state,
                                    erlang:element(2, Inactive),
                                    erlang:element(3, Inactive),
                                    erlang:element(4, Inactive),
                                    erlang:element(5, Inactive),
                                    erlang:element(6, Inactive),
                                    erlang:element(7, Inactive),
                                    [Sequence@1 | erlang:element(8, Inactive)],
                                    erlang:element(9, Inactive),
                                    erlang:element(10, Inactive),
                                    erlang:element(11, Inactive)}
                            )
                    end,
                    {Refilled, Start_commands} = fill_slots(Emitted_state, []),
                    finish_if_drained(
                        Refilled,
                        lists:append(Emit_commands, Start_commands)
                    )
            end;

        {complete_failure, Sequence@2} ->
            case remove(erlang:element(7, State), Sequence@2) of
                {false, _} ->
                    {State, []};

                {true, Remaining_active@1} ->
                    Cancel_commands = cancel_commands(Remaining_active@1),
                    {{state,
                            erlang:element(2, State),
                            erlang:element(3, State),
                            erlang:element(4, State),
                            erlang:element(5, State),
                            [],
                            [],
                            [],
                            erlang:element(9, State),
                            erlang:element(10, State),
                            failed},
                        [fail | Cancel_commands]}
            end;

        finish_input ->
            finish_if_drained(
                {state,
                    erlang:element(2, State),
                    erlang:element(3, State),
                    erlang:element(4, State),
                    erlang:element(5, State),
                    erlang:element(6, State),
                    erlang:element(7, State),
                    erlang:element(8, State),
                    erlang:element(9, State),
                    true,
                    erlang:element(11, State)},
                []
            );

        cancel ->
            {{state,
                    erlang:element(2, State),
                    erlang:element(3, State),
                    erlang:element(4, State),
                    erlang:element(5, State),
                    [],
                    [],
                    [],
                    erlang:element(9, State),
                    erlang:element(10, State),
                    cancelled},
                cancel_commands(erlang:element(7, State))}
    end.

-file("src/rx/flow_model.gleam", 66).
-spec transition(state(), event()) -> {state(), list(command())}.
transition(State, Event) ->
    case erlang:element(11, State) of
        failed ->
            {State, []};

        cancelled ->
            {State, []};

        drained ->
            {State, []};

        running ->
            transition_running(State, Event)
    end.

-file("src/rx/flow_model.gleam", 274).
-spec prefix_loop(integer(), integer(), list(integer())) -> list(integer()).
prefix_loop(Current, Length, Reversed) ->
    case Current >= Length of
        true ->
            Reversed;

        false ->
            prefix_loop(Current + 1, Length, [Current | Reversed])
    end.

-file("src/rx/flow_model.gleam", 270).
-spec prefix(integer()) -> list(integer()).
prefix(Length) ->
    _pipe = prefix_loop(0, Length, []),
    lists:reverse(_pipe).

-file("src/rx/flow_model.gleam", 259).
-spec count_occurrences(list(integer()), integer()) -> integer().
count_occurrences(Items, Wanted) ->
    case Items of
        [] ->
            0;

        [First | Rest] ->
            case First =:= Wanted of
                true ->
                    1 + count_occurrences(Rest, Wanted);

                false ->
                    count_occurrences(Rest, Wanted)
            end
    end.

-file("src/rx/flow_model.gleam", 246).
-spec accepted_exactly_once(state(), integer()) -> boolean().
accepted_exactly_once(State, Sequence) ->
    case Sequence >= erlang:element(4, State) of
        true ->
            true;

        false ->
            ((((count_occurrences(erlang:element(6, State), Sequence) + count_occurrences(
                erlang:element(7, State),
                Sequence
            ))
            + count_occurrences(erlang:element(8, State), Sequence))
            + count_occurrences(erlang:element(9, State), Sequence))
            =:= 1)
            andalso accepted_exactly_once(State, Sequence + 1)
    end.

-file("src/rx/flow_model.gleam", 316).
-spec all_below(list(integer()), integer()) -> boolean().
all_below(Items, Bound) ->
    case Items of
        [] ->
            true;

        [First | Rest] ->
            ((First >= 0) andalso (First < Bound)) andalso all_below(
                Rest,
                Bound
            )
    end.

-file("src/rx/flow_model.gleam", 323).
-spec contains(list(integer()), integer()) -> boolean().
contains(Items, Wanted) ->
    case Items of
        [] ->
            false;

        [First | Rest] ->
            (First =:= Wanted) orelse contains(Rest, Wanted)
    end.

-file("src/rx/flow_model.gleam", 309).
-spec disjoint(list(integer()), list(integer())) -> boolean().
disjoint(Left, Right) ->
    case Left of
        [] ->
            true;

        [First | Rest] ->
            not contains(Right, First) andalso disjoint(Rest, Right)
    end.

-file("src/rx/flow_model.gleam", 302).
-spec unique(list(integer())) -> boolean().
unique(Items) ->
    case Items of
        [] ->
            true;

        [First | Rest] ->
            not contains(Rest, First) andalso unique(Rest)
    end.

-file("src/rx/flow_model.gleam", 75).
-spec invariants_hold(state()) -> boolean().
invariants_hold(State) ->
    Capacity_ok = (erlang:element(2, State) > 0) andalso (erlang:length(
        erlang:element(7, State)
    )
    =< erlang:element(2, State)),
    Sequence_bounds = ((erlang:element(4, State) >= 0) andalso (erlang:element(
        5,
        State
    )
    >= 0))
    andalso (erlang:element(5, State) =< erlang:element(4, State)),
    Unique_sets = ((unique(erlang:element(6, State)) andalso unique(
        erlang:element(7, State)
    ))
    andalso unique(erlang:element(8, State)))
    andalso unique(erlang:element(9, State)),
    Disjoint_sets = ((((disjoint(
        erlang:element(6, State),
        erlang:element(7, State)
    )
    andalso disjoint(erlang:element(6, State), erlang:element(8, State)))
    andalso disjoint(erlang:element(6, State), erlang:element(9, State)))
    andalso disjoint(erlang:element(7, State), erlang:element(8, State)))
    andalso disjoint(erlang:element(7, State), erlang:element(9, State)))
    andalso disjoint(erlang:element(8, State), erlang:element(9, State)),
    Known_sequences = ((all_below(
        erlang:element(6, State),
        erlang:element(4, State)
    )
    andalso all_below(erlang:element(7, State), erlang:element(4, State)))
    andalso all_below(erlang:element(8, State), erlang:element(4, State)))
    andalso all_below(erlang:element(9, State), erlang:element(4, State)),
    Running_conserves_work = case erlang:element(11, State) of
        running ->
            accepted_exactly_once(State, 0);

        _ ->
            true
    end,
    Ordering_consistent = case erlang:element(3, State) of
        input_order ->
            (erlang:element(9, State) =:= prefix(erlang:element(5, State)))
            andalso (erlang:element(5, State) =:= erlang:length(
                erlang:element(9, State)
            ));

        completion_order ->
            erlang:element(8, State) =:= []
    end,
    Terminal_is_empty = case erlang:element(11, State) of
        running ->
            true;

        failed ->
            empty_work(State);

        cancelled ->
            empty_work(State);

        drained ->
            (empty_work(State) andalso erlang:element(10, State)) andalso (erlang:length(
                erlang:element(9, State)
            )
            =:= erlang:element(4, State))
    end,
    ((((((Capacity_ok andalso Sequence_bounds) andalso Unique_sets) andalso Disjoint_sets)
    andalso Known_sequences)
    andalso Running_conserves_work)
    andalso Ordering_consistent)
    andalso Terminal_is_empty.

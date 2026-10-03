-module(lifecycle_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/lifecycle_test.gleam").
-export([main/0, exhaustive_lifecycle_sequences_up_to_length_six_test/0, terminal_with_teardown_closes_and_releases_test/0, terminal_before_teardown_releases_when_teardown_arrives_test/0]).

-file("test/lifecycle_test.gleam", 7).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/lifecycle_test.gleam", 120).
-spec count_stored_teardowns(list(rx@lifecycle:command())) -> integer().
count_stored_teardowns(Commands) ->
    case Commands of
        [] ->
            0;

        [First | Rest] ->
            Current = case First of
                run_stored_teardown ->
                    1;

                _ ->
                    0
            end,
            Current + count_stored_teardowns(Rest)
    end.

-file("test/lifecycle_test.gleam", 101).
-spec trace_is_safe(
    list(rx@lifecycle:action()),
    rx@lifecycle:state(),
    integer()
) -> boolean().
trace_is_safe(Actions, State, Stored_teardown_runs) ->
    case Actions of
        [] ->
            Stored_teardown_runs =< 1;

        [Action | Rest] ->
            {Next_state, Commands} = rx@lifecycle:transition(State, Action),
            Next_runs = Stored_teardown_runs + count_stored_teardowns(Commands),
            Absorbing = case State of
                closed ->
                    Next_state =:= closed;

                {active, _, _} ->
                    true
            end,
            (Absorbing andalso (Next_runs =< 1)) andalso trace_is_safe(
                Rest,
                Next_state,
                Next_runs
            )
    end.

-file("test/lifecycle_test.gleam", 90).
-spec assert_all_safe(list(list(rx@lifecycle:action()))) -> nil.
assert_all_safe(Sequences) ->
    case Sequences of
        [] ->
            nil;

        [Sequence | Rest] ->
            _pipe = trace_is_safe(Sequence, rx@lifecycle:initial(), 0),
            gleeunit@should:equal(_pipe, true),
            assert_all_safe(Rest)
    end.

-file("test/lifecycle_test.gleam", 42).
-spec alphabet() -> list(rx@lifecycle:action()).
alphabet() ->
    [install_teardown,
        {notify, next_kind},
        {notify, error_kind},
        {notify, complete_kind},
        cancel].

-file("test/lifecycle_test.gleam", 80).
-spec attach(list(rx@lifecycle:action()), list(rx@lifecycle:action())) -> list(list(rx@lifecycle:action())).
attach(Sequence, Symbols) ->
    case Symbols of
        [] ->
            [];

        [Symbol | Rest] ->
            [[Symbol | Sequence] | attach(Sequence, Rest)]
    end.

-file("test/lifecycle_test.gleam", 69).
-spec prepend_each(
    list(list(rx@lifecycle:action())),
    list(rx@lifecycle:action())
) -> list(list(rx@lifecycle:action())).
prepend_each(Sequences, Symbols) ->
    case Sequences of
        [] ->
            [];

        [Sequence | Rest] ->
            lists:append(attach(Sequence, Symbols), prepend_each(Rest, Symbols))
    end.

-file("test/lifecycle_test.gleam", 56).
-spec generate(list(list(rx@lifecycle:action())), integer()) -> list(list(rx@lifecycle:action())).
generate(Current, Remaining) ->
    case Remaining of
        0 ->
            Current;

        _ ->
            Extended = prepend_each(Current, alphabet()),
            generate(lists:append(Current, Extended), Remaining - 1)
    end.

-file("test/lifecycle_test.gleam", 52).
-spec all_sequences(integer()) -> list(list(rx@lifecycle:action())).
all_sequences(Max_length) ->
    generate([[]], Max_length).

-file("test/lifecycle_test.gleam", 11).
-spec exhaustive_lifecycle_sequences_up_to_length_six_test() -> nil.
exhaustive_lifecycle_sequences_up_to_length_six_test() ->
    _pipe = all_sequences(6),
    assert_all_safe(_pipe).

-file("test/lifecycle_test.gleam", 16).
-spec terminal_with_teardown_closes_and_releases_test() -> nil.
terminal_with_teardown_closes_and_releases_test() ->
    {Installed, Install_commands} = rx@lifecycle:transition(
        rx@lifecycle:initial(),
        install_teardown
    ),
    _pipe = Install_commands,
    gleeunit@should:equal(_pipe, []),
    {Closed, Commands} = rx@lifecycle:transition(
        Installed,
        {notify, complete_kind}
    ),
    _pipe@1 = Closed,
    gleeunit@should:equal(_pipe@1, closed),
    _pipe@2 = Commands,
    gleeunit@should:equal(_pipe@2, [deliver, run_stored_teardown]).

-file("test/lifecycle_test.gleam", 28).
-spec terminal_before_teardown_releases_when_teardown_arrives_test() -> nil.
terminal_before_teardown_releases_when_teardown_arrives_test() ->
    {Terminated, Commands} = rx@lifecycle:transition(
        rx@lifecycle:initial(),
        {notify, complete_kind}
    ),
    _pipe = Commands,
    gleeunit@should:equal(_pipe, [deliver]),
    {Closed, Release_commands} = rx@lifecycle:transition(
        Terminated,
        install_teardown
    ),
    _pipe@1 = Closed,
    gleeunit@should:equal(_pipe@1, closed),
    _pipe@2 = Release_commands,
    gleeunit@should:equal(_pipe@2, [run_incoming_teardown]).

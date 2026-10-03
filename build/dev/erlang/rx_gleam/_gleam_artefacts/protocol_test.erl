-module(protocol_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/protocol_test.gleam").
-export([main/0, valid_open_sequences_test/0, valid_complete_sequence_test/0, valid_error_sequence_test/0, next_after_complete_is_rejected_test/0, complete_after_error_is_rejected_test/0, exhaustive_sequences_up_to_length_six_test/0]).

-file("test/protocol_test.gleam", 6).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/protocol_test.gleam", 52).
-spec protocol_expect(list(rx@protocol:notification(integer(), binary()))) -> {ok,
        rx@protocol:phase()} |
    {error, rx@protocol:protocol_error()}.
protocol_expect(Sequence) ->
    rx@protocol:validate(Sequence).

-file("test/protocol_test.gleam", 10).
-spec valid_open_sequences_test() -> nil.
valid_open_sequences_test() ->
    _pipe = protocol_expect([{on_next, 1}, {on_next, 2}, {on_next, 3}]),
    gleeunit@should:equal(_pipe, {ok, open}).

-file("test/protocol_test.gleam", 19).
-spec valid_complete_sequence_test() -> nil.
valid_complete_sequence_test() ->
    _pipe = protocol_expect([{on_next, 1}, {on_next, 2}, on_complete]),
    gleeunit@should:equal(_pipe, {ok, terminated}).

-file("test/protocol_test.gleam", 28).
-spec valid_error_sequence_test() -> nil.
valid_error_sequence_test() ->
    _pipe = protocol_expect([{on_next, 1}, {on_error, <<"boom"/utf8>>}]),
    gleeunit@should:equal(_pipe, {ok, terminated}).

-file("test/protocol_test.gleam", 33).
-spec next_after_complete_is_rejected_test() -> nil.
next_after_complete_is_rejected_test() ->
    _pipe = protocol_expect([{on_next, 1}, on_complete, {on_next, 2}]),
    gleeunit@should:equal(_pipe, {error, notification_after_termination}).

-file("test/protocol_test.gleam", 42).
-spec complete_after_error_is_rejected_test() -> nil.
complete_after_error_is_rejected_test() ->
    _pipe = protocol_expect([{on_error, <<"boom"/utf8>>}, on_complete]),
    gleeunit@should:equal(_pipe, {error, duplicate_termination}).

-file("test/protocol_test.gleam", 117).
-spec reference_from(
    rx@protocol:phase(),
    list(rx@protocol:notification(integer(), binary()))
) -> {ok, rx@protocol:phase()} | {error, rx@protocol:protocol_error()}.
reference_from(Phase, Sequence) ->
    case Sequence of
        [] ->
            {ok, Phase};

        [Item | Rest] ->
            case Phase of
                open ->
                    case Item of
                        {on_next, _} ->
                            reference_from(open, Rest);

                        {on_error, _} ->
                            reference_from(terminated, Rest);

                        on_complete ->
                            reference_from(terminated, Rest)
                    end;

                terminated ->
                    case Item of
                        {on_next, _} ->
                            {error, notification_after_termination};

                        {on_error, _} ->
                            {error, duplicate_termination};

                        on_complete ->
                            {error, duplicate_termination}
                    end
            end
    end.

-file("test/protocol_test.gleam", 111).
-spec reference_validate(list(rx@protocol:notification(integer(), binary()))) -> {ok,
        rx@protocol:phase()} |
    {error, rx@protocol:protocol_error()}.
reference_validate(Sequence) ->
    reference_from(open, Sequence).

-file("test/protocol_test.gleam", 98).
-spec assert_model_agreement(
    list(list(rx@protocol:notification(integer(), binary())))
) -> nil.
assert_model_agreement(Sequences) ->
    case Sequences of
        [] ->
            nil;

        [Sequence | Rest] ->
            _pipe = rx@protocol:validate(Sequence),
            gleeunit@should:equal(_pipe, reference_validate(Sequence)),
            assert_model_agreement(Rest)
    end.

-file("test/protocol_test.gleam", 56).
-spec alphabet() -> list(rx@protocol:notification(integer(), binary())).
alphabet() ->
    [{on_next, 1}, {on_error, <<"error"/utf8>>}, on_complete].

-file("test/protocol_test.gleam", 88).
-spec attach(
    list(rx@protocol:notification(integer(), binary())),
    list(rx@protocol:notification(integer(), binary()))
) -> list(list(rx@protocol:notification(integer(), binary()))).
attach(Sequence, Symbols) ->
    case Symbols of
        [] ->
            [];

        [Symbol | Rest] ->
            [[Symbol | Sequence] | attach(Sequence, Rest)]
    end.

-file("test/protocol_test.gleam", 77).
-spec prepend_each(
    list(list(rx@protocol:notification(integer(), binary()))),
    list(rx@protocol:notification(integer(), binary()))
) -> list(list(rx@protocol:notification(integer(), binary()))).
prepend_each(Sequences, Symbols) ->
    case Sequences of
        [] ->
            [];

        [Sequence | Rest] ->
            lists:append(attach(Sequence, Symbols), prepend_each(Rest, Symbols))
    end.

-file("test/protocol_test.gleam", 64).
-spec generate(
    list(list(rx@protocol:notification(integer(), binary()))),
    integer()
) -> list(list(rx@protocol:notification(integer(), binary()))).
generate(Current, Remaining) ->
    case Remaining of
        0 ->
            Current;

        _ ->
            Extended = prepend_each(Current, alphabet()),
            generate(lists:append(Current, Extended), Remaining - 1)
    end.

-file("test/protocol_test.gleam", 60).
-spec all_sequences(integer()) -> list(list(rx@protocol:notification(integer(), binary()))).
all_sequences(Max_length) ->
    generate([[]], Max_length).

-file("test/protocol_test.gleam", 47).
-spec exhaustive_sequences_up_to_length_six_test() -> nil.
exhaustive_sequences_up_to_length_six_test() ->
    _pipe = all_sequences(6),
    assert_model_agreement(_pipe).

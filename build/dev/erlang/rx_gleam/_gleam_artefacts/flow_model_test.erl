-module(flow_model_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/flow_model_test.gleam").
-export([main/0, concat_reference_model_is_fifo_test/0, concurrent_completion_order_model_emits_ready_result_test/0, completion_order_records_each_accepted_item_exactly_once_test/0, concurrent_input_order_model_buffers_early_completion_test/0, failure_is_absorbing_and_cancels_other_active_work_test/0, failure_preserves_only_already_emitted_history_test/0, exhaustively_preserves_invariants_concurrency_one_test/0, exhaustively_preserves_invariants_concurrency_two_ordered_test/0, exhaustively_preserves_invariants_concurrency_two_unordered_test/0]).

-file("test/flow_model_test.gleam", 5).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/flow_model_test.gleam", 9).
-spec concat_reference_model_is_fifo_test() -> nil.
concat_reference_model_is_fifo_test() ->
    State0 = rx@flow_model:new(1, input_order),
    {State1, Commands1} = rx@flow_model:transition(State0, enqueue),
    _pipe = Commands1,
    gleeunit@should:equal(_pipe, [{start, 0}]),
    {State2, Commands2} = rx@flow_model:transition(State1, enqueue),
    _pipe@1 = Commands2,
    gleeunit@should:equal(_pipe@1, []),
    {State3, Commands3} = rx@flow_model:transition(State2, enqueue),
    _pipe@2 = Commands3,
    gleeunit@should:equal(_pipe@2, []),
    {State4, Commands4} = rx@flow_model:transition(
        State3,
        {complete_success, 0}
    ),
    _pipe@3 = Commands4,
    gleeunit@should:equal(_pipe@3, [{emit, 0}, {start, 1}]),
    {State5, Commands5} = rx@flow_model:transition(
        State4,
        {complete_success, 1}
    ),
    _pipe@4 = Commands5,
    gleeunit@should:equal(_pipe@4, [{emit, 1}, {start, 2}]),
    {State6, _} = rx@flow_model:transition(State5, finish_input),
    {State7, Commands7} = rx@flow_model:transition(
        State6,
        {complete_success, 2}
    ),
    _pipe@5 = Commands7,
    gleeunit@should:equal(_pipe@5, [{emit, 2}, drain]),
    _pipe@6 = erlang:element(11, State7),
    gleeunit@should:equal(_pipe@6, drained),
    _pipe@7 = erlang:element(9, State7),
    gleeunit@should:equal(_pipe@7, [0, 1, 2]),
    _pipe@8 = rx@flow_model:invariants_hold(State7),
    gleeunit@should:equal(_pipe@8, true).

-file("test/flow_model_test.gleam", 34).
-spec concurrent_completion_order_model_emits_ready_result_test() -> nil.
concurrent_completion_order_model_emits_ready_result_test() ->
    State0 = rx@flow_model:new(2, completion_order),
    {State1, _} = rx@flow_model:transition(State0, enqueue),
    {State2, _} = rx@flow_model:transition(State1, enqueue),
    {State3, _} = rx@flow_model:transition(State2, enqueue),
    {State4, Commands} = rx@flow_model:transition(State3, {complete_success, 1}),
    _pipe = Commands,
    gleeunit@should:equal(_pipe, [{emit, 1}, {start, 2}]),
    _pipe@1 = erlang:element(7, State4),
    gleeunit@should:equal(_pipe@1, [2, 0]),
    _pipe@2 = erlang:element(9, State4),
    gleeunit@should:equal(_pipe@2, [1]),
    _pipe@3 = rx@flow_model:invariants_hold(State4),
    gleeunit@should:equal(_pipe@3, true).

-file("test/flow_model_test.gleam", 47).
-spec completion_order_records_each_accepted_item_exactly_once_test() -> nil.
completion_order_records_each_accepted_item_exactly_once_test() ->
    State0 = rx@flow_model:new(2, completion_order),
    {State1, _} = rx@flow_model:transition(State0, enqueue),
    {State2, _} = rx@flow_model:transition(State1, enqueue),
    {State3, _} = rx@flow_model:transition(State2, enqueue),
    {State4, _} = rx@flow_model:transition(State3, finish_input),
    {State5, Commands5} = rx@flow_model:transition(
        State4,
        {complete_success, 1}
    ),
    _pipe = Commands5,
    gleeunit@should:equal(_pipe, [{emit, 1}, {start, 2}]),
    {State6, Commands6} = rx@flow_model:transition(
        State5,
        {complete_success, 2}
    ),
    _pipe@1 = Commands6,
    gleeunit@should:equal(_pipe@1, [{emit, 2}]),
    {State7, Commands7} = rx@flow_model:transition(
        State6,
        {complete_success, 0}
    ),
    _pipe@2 = Commands7,
    gleeunit@should:equal(_pipe@2, [{emit, 0}, drain]),
    _pipe@3 = erlang:element(9, State7),
    gleeunit@should:equal(_pipe@3, [1, 2, 0]),
    _pipe@4 = erlang:element(11, State7),
    gleeunit@should:equal(_pipe@4, drained),
    _pipe@5 = rx@flow_model:invariants_hold(State7),
    gleeunit@should:equal(_pipe@5, true).

-file("test/flow_model_test.gleam", 64).
-spec concurrent_input_order_model_buffers_early_completion_test() -> nil.
concurrent_input_order_model_buffers_early_completion_test() ->
    State0 = rx@flow_model:new(2, input_order),
    {State1, _} = rx@flow_model:transition(State0, enqueue),
    {State2, _} = rx@flow_model:transition(State1, enqueue),
    {State3, Commands3} = rx@flow_model:transition(
        State2,
        {complete_success, 1}
    ),
    _pipe = Commands3,
    gleeunit@should:equal(_pipe, []),
    _pipe@1 = erlang:element(8, State3),
    gleeunit@should:equal(_pipe@1, [1]),
    _pipe@2 = erlang:element(9, State3),
    gleeunit@should:equal(_pipe@2, []),
    {State4, Commands4} = rx@flow_model:transition(
        State3,
        {complete_success, 0}
    ),
    _pipe@3 = Commands4,
    gleeunit@should:equal(_pipe@3, [{emit, 0}, {emit, 1}]),
    _pipe@4 = erlang:element(8, State4),
    gleeunit@should:equal(_pipe@4, []),
    _pipe@5 = erlang:element(5, State4),
    gleeunit@should:equal(_pipe@5, 2),
    _pipe@6 = erlang:element(9, State4),
    gleeunit@should:equal(_pipe@6, [0, 1]),
    _pipe@7 = rx@flow_model:invariants_hold(State4),
    gleeunit@should:equal(_pipe@7, true).

-file("test/flow_model_test.gleam", 82).
-spec failure_is_absorbing_and_cancels_other_active_work_test() -> nil.
failure_is_absorbing_and_cancels_other_active_work_test() ->
    State0 = rx@flow_model:new(2, completion_order),
    {State1, _} = rx@flow_model:transition(State0, enqueue),
    {State2, _} = rx@flow_model:transition(State1, enqueue),
    {Failed, Commands} = rx@flow_model:transition(State2, {complete_failure, 1}),
    _pipe = Commands,
    gleeunit@should:equal(_pipe, [fail, {cancel_active, 0}]),
    _pipe@1 = erlang:element(11, Failed),
    gleeunit@should:equal(_pipe@1, failed),
    _pipe@2 = rx@flow_model:invariants_hold(Failed),
    gleeunit@should:equal(_pipe@2, true),
    {Still_failed, Late_commands} = rx@flow_model:transition(
        Failed,
        {complete_success, 0}
    ),
    _pipe@3 = Still_failed,
    gleeunit@should:equal(_pipe@3, Failed),
    _pipe@4 = Late_commands,
    gleeunit@should:equal(_pipe@4, []).

-file("test/flow_model_test.gleam", 98).
-spec failure_preserves_only_already_emitted_history_test() -> nil.
failure_preserves_only_already_emitted_history_test() ->
    State0 = rx@flow_model:new(2, completion_order),
    {State1, _} = rx@flow_model:transition(State0, enqueue),
    {State2, _} = rx@flow_model:transition(State1, enqueue),
    {State3, _} = rx@flow_model:transition(State2, enqueue),
    {State4, _} = rx@flow_model:transition(State3, {complete_success, 1}),
    _pipe = erlang:element(9, State4),
    gleeunit@should:equal(_pipe, [1]),
    {Failed, Commands} = rx@flow_model:transition(State4, {complete_failure, 0}),
    _pipe@1 = Commands,
    gleeunit@should:equal(_pipe@1, [fail, {cancel_active, 2}]),
    _pipe@2 = erlang:element(9, Failed),
    gleeunit@should:equal(_pipe@2, [1]),
    _pipe@3 = erlang:element(6, Failed),
    gleeunit@should:equal(_pipe@3, []),
    _pipe@4 = erlang:element(7, Failed),
    gleeunit@should:equal(_pipe@4, []),
    _pipe@5 = erlang:element(8, Failed),
    gleeunit@should:equal(_pipe@5, []),
    _pipe@6 = rx@flow_model:invariants_hold(Failed),
    gleeunit@should:equal(_pipe@6, true).

-file("test/flow_model_test.gleam", 151).
-spec alphabet() -> list(rx@flow_model:event()).
alphabet() ->
    [enqueue,
        {complete_success, 0},
        {complete_success, 1},
        {complete_failure, 0},
        {complete_failure, 1},
        finish_input,
        cancel].

-file("test/flow_model_test.gleam", 136).
-spec explore_events(
    rx@flow_model:state(),
    list(rx@flow_model:event()),
    integer()
) -> nil.
explore_events(State, Events, Remaining) ->
    case Events of
        [] ->
            nil;

        [Event | Rest] ->
            {Next, _} = rx@flow_model:transition(State, Event),
            _ = explore(Next, Remaining - 1),
            explore_events(State, Rest, Remaining)
    end.

-file("test/flow_model_test.gleam", 127).
-spec explore(rx@flow_model:state(), integer()) -> nil.
explore(State, Remaining) ->
    _pipe = rx@flow_model:invariants_hold(State),
    gleeunit@should:equal(_pipe, true),
    case Remaining of
        0 ->
            nil;

        _ ->
            explore_events(State, alphabet(), Remaining)
    end.

-file("test/flow_model_test.gleam", 115).
-spec exhaustively_preserves_invariants_concurrency_one_test() -> nil.
exhaustively_preserves_invariants_concurrency_one_test() ->
    explore(rx@flow_model:new(1, input_order), 6).

-file("test/flow_model_test.gleam", 119).
-spec exhaustively_preserves_invariants_concurrency_two_ordered_test() -> nil.
exhaustively_preserves_invariants_concurrency_two_ordered_test() ->
    explore(rx@flow_model:new(2, input_order), 6).

-file("test/flow_model_test.gleam", 123).
-spec exhaustively_preserves_invariants_concurrency_two_unordered_test() -> nil.
exhaustively_preserves_invariants_concurrency_two_unordered_test() ->
    explore(rx@flow_model:new(2, completion_order), 6).

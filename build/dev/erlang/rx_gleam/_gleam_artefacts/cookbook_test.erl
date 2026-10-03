-module(cookbook_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/cookbook_test.gleam").
-export([main/0, cookbook_01_emit_one_value_test/0, cookbook_02_emit_finite_list_test/0, cookbook_03_complete_without_values_test/0, cookbook_04_fail_immediately_test/0, cookbook_05_transform_values_test/0, cookbook_06_filter_values_test/0, cookbook_07_tap_inspects_and_forwards_test/0, cookbook_08_explicit_observer_handlers_test/0, cookbook_09_cancellation_is_idempotent_test/0, cookbook_10_future_pure_test/0, cookbook_11_future_fail_test/0, cookbook_12_future_map_test/0, cookbook_13_future_await_continuation_test/0, cookbook_14_future_to_stream_test/0, cookbook_15_async_source_can_emit_later_test/0, cookbook_16_concat_map_is_fifo_test/0, cookbook_17_merge_map_bounds_concurrency_test/0, cookbook_18_map_ordered_buffers_early_results_test/0, cookbook_19_async_filter_preserves_order_test/0, cookbook_20_fail_fast_cancels_and_ignores_late_result_test/0]).
-export_type([output_event/0, controlled_work/0]).

-type output_event() :: {value, integer()} | {failed, binary()} | completed.

-type controlled_work() :: {controlled_work,
        integer(),
        fun(({ok, integer()} | {error, binary()}) -> nil)}.

-file("test/cookbook_test.gleam", 19).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/cookbook_test.gleam", 366).
-spec receive_output(gleam@erlang@process:subject(output_event())) -> output_event().
receive_output(Outputs) ->
    Event@1 = case gleam@erlang@process:'receive'(Outputs, 1000) of
        {ok, Event} -> Event;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"receive_output"/utf8>>,
                        line => 367,
                        value => _assert_fail,
                        start => 11149,
                        'end' => 11216,
                        pattern_start => 11160,
                        pattern_end => 11169})
    end,
    Event@1.

-file("test/cookbook_test.gleam", 343).
-spec expect_outputs(
    gleam@erlang@process:subject(output_event()),
    list(output_event())
) -> nil.
expect_outputs(Outputs, Expected) ->
    case Expected of
        [] ->
            nil;

        [First | Rest] ->
            _pipe = receive_output(Outputs),
            gleeunit@should:equal(_pipe, First),
            expect_outputs(Outputs, Rest)
    end.

-file("test/cookbook_test.gleam", 356).
-spec output_observer(gleam@erlang@process:subject(output_event())) -> rx:observer(integer(), binary()).
output_observer(Outputs) ->
    rx:observer(
        fun(Value) -> gleam@erlang@process:send(Outputs, {value, Value}) end,
        fun(Reason) -> gleam@erlang@process:send(Outputs, {failed, Reason}) end,
        fun() -> gleam@erlang@process:send(Outputs, completed) end
    ).

-file("test/cookbook_test.gleam", 329).
-spec assert_stream(rx:observable(integer(), binary()), list(output_event())) -> nil.
assert_stream(Source, Expected) ->
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"assert_stream"/utf8>>,
                        line => 334,
                        value => _assert_fail,
                        start => 10306,
                        'end' => 10347,
                        pattern_start => 10317,
                        pattern_end => 10329})
    end,
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"assert_stream"/utf8>>,
                        line => 335,
                        value => _assert_fail@1,
                        start => 10350,
                        'end' => 10440,
                        pattern_start => 10361,
                        pattern_end => 10377})
    end,
    expect_outputs(Outputs, Expected),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 23).
-spec cookbook_01_emit_one_value_test() -> nil.
cookbook_01_emit_one_value_test() ->
    assert_stream(rx:'of'(7), [{value, 7}, completed]).

-file("test/cookbook_test.gleam", 27).
-spec cookbook_02_emit_finite_list_test() -> nil.
cookbook_02_emit_finite_list_test() ->
    assert_stream(
        rx:from_list([1, 2, 3]),
        [{value, 1}, {value, 2}, {value, 3}, completed]
    ).

-file("test/cookbook_test.gleam", 36).
-spec cookbook_03_complete_without_values_test() -> nil.
cookbook_03_complete_without_values_test() ->
    Source = rx:empty(),
    assert_stream(Source, [completed]).

-file("test/cookbook_test.gleam", 41).
-spec cookbook_04_fail_immediately_test() -> nil.
cookbook_04_fail_immediately_test() ->
    Source = rx:fail(<<"boom"/utf8>>),
    assert_stream(Source, [{failed, <<"boom"/utf8>>}]).

-file("test/cookbook_test.gleam", 46).
-spec cookbook_05_transform_values_test() -> nil.
cookbook_05_transform_values_test() ->
    _pipe = rx:from_list([1, 2, 3]),
    _pipe@1 = rx:map(_pipe, fun(Value) -> Value * 10 end),
    assert_stream(_pipe@1, [{value, 10}, {value, 20}, {value, 30}, completed]).

-file("test/cookbook_test.gleam", 52).
-spec cookbook_06_filter_values_test() -> nil.
cookbook_06_filter_values_test() ->
    _pipe = rx:from_list([1, 2, 3, 4]),
    _pipe@1 = rx:filter(_pipe, fun(Value) -> (Value rem 2) =:= 0 end),
    assert_stream(_pipe@1, [{value, 2}, {value, 4}, completed]).

-file("test/cookbook_test.gleam", 58).
-spec cookbook_07_tap_inspects_and_forwards_test() -> nil.
cookbook_07_tap_inspects_and_forwards_test() ->
    Inspected = gleam@erlang@process:new_subject(),
    Source = begin
        _pipe = rx:'of'(9),
        rx:tap(
            _pipe,
            fun(Value) -> gleam@erlang@process:send(Inspected, Value) end
        )
    end,
    assert_stream(Source, [{value, 9}, completed]),
    _pipe@1 = gleam@erlang@process:'receive'(Inspected, 1000),
    gleeunit@should:equal(_pipe@1, {ok, 9}).

-file("test/cookbook_test.gleam", 69).
-spec cookbook_08_explicit_observer_handlers_test() -> nil.
cookbook_08_explicit_observer_handlers_test() ->
    Values = gleam@erlang@process:new_subject(),
    Errors = gleam@erlang@process:new_subject(),
    Completions = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_08_explicit_observer_handlers_test"/utf8>>,
                        line => 73,
                        value => _assert_fail,
                        start => 1722,
                        'end' => 1763,
                        pattern_start => 1733,
                        pattern_end => 1745})
    end,
    Source = rx:'of'(5),
    Observer = rx:observer(
        fun(Value) -> gleam@erlang@process:send(Values, Value) end,
        fun(Reason) -> gleam@erlang@process:send(Errors, Reason) end,
        fun() -> gleam@erlang@process:send(Completions, nil) end
    ),
    Subscription@1 = case rx:subscribe(Source, Runtime_@1, Observer) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_08_explicit_observer_handlers_test"/utf8>>,
                        line => 82,
                        value => _assert_fail@1,
                        start => 2006,
                        'end' => 2076,
                        pattern_start => 2017,
                        pattern_end => 2033})
    end,
    _pipe = gleam@erlang@process:'receive'(Values, 1000),
    gleeunit@should:equal(_pipe, {ok, 5}),
    _pipe@1 = gleam@erlang@process:'receive'(Completions, 1000),
    gleeunit@should:equal(_pipe@1, {ok, nil}),
    _pipe@2 = gleam@erlang@process:'receive'(Errors, 20),
    gleeunit@should:equal(_pipe@2, {error, nil}),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 91).
-spec cookbook_09_cancellation_is_idempotent_test() -> nil.
cookbook_09_cancellation_is_idempotent_test() ->
    Teardowns = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_09_cancellation_is_idempotent_test"/utf8>>,
                        line => 93,
                        value => _assert_fail,
                        start => 2451,
                        'end' => 2492,
                        pattern_start => 2462,
                        pattern_end => 2474})
    end,
    Source = rx:create(
        fun(_) -> fun() -> gleam@erlang@process:send(Teardowns, nil) end end
    ),
    Observer = rx:observer(
        fun(_) -> nil end,
        fun(_) -> nil end,
        fun() -> nil end
    ),
    Subscription@1 = case rx:subscribe(Source, Runtime_@1, Observer) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_09_cancellation_is_idempotent_test"/utf8>>,
                        line => 97,
                        value => _assert_fail@1,
                        start => 2674,
                        'end' => 2744,
                        pattern_start => 2685,
                        pattern_end => 2701})
    end,
    rx:unsubscribe(Subscription@1),
    rx:unsubscribe(Subscription@1),
    _pipe = gleam@erlang@process:'receive'(Teardowns, 1000),
    gleeunit@should:equal(_pipe, {ok, nil}),
    _pipe@1 = gleam@erlang@process:'receive'(Teardowns, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 107).
-spec cookbook_10_future_pure_test() -> nil.
cookbook_10_future_pure_test() ->
    Results = gleam@erlang@process:new_subject(),
    Value = rx@future:pure(42),
    _ = rx@future:run(
        Value,
        fun(Result) -> gleam@erlang@process:send(Results, Result) end
    ),
    _pipe = gleam@erlang@process:'receive'(Results, 1000),
    gleeunit@should:equal(_pipe, {ok, {ok, 42}}).

-file("test/cookbook_test.gleam", 116).
-spec cookbook_11_future_fail_test() -> nil.
cookbook_11_future_fail_test() ->
    Results = gleam@erlang@process:new_subject(),
    Value = rx@future:fail(<<"nope"/utf8>>),
    _ = rx@future:run(
        Value,
        fun(Result) -> gleam@erlang@process:send(Results, Result) end
    ),
    _pipe = gleam@erlang@process:'receive'(Results, 1000),
    gleeunit@should:equal(_pipe, {ok, {error, <<"nope"/utf8>>}}).

-file("test/cookbook_test.gleam", 125).
-spec cookbook_12_future_map_test() -> nil.
cookbook_12_future_map_test() ->
    Results = gleam@erlang@process:new_subject(),
    Value = begin
        _pipe = rx@future:pure(4),
        rx@future:map(_pipe, fun(Number) -> Number * 3 end)
    end,
    _ = rx@future:run(
        Value,
        fun(Result) -> gleam@erlang@process:send(Results, Result) end
    ),
    _pipe@1 = gleam@erlang@process:'receive'(Results, 1000),
    gleeunit@should:equal(_pipe@1, {ok, {ok, 12}}).

-file("test/cookbook_test.gleam", 136).
-spec cookbook_13_future_await_continuation_test() -> nil.
cookbook_13_future_await_continuation_test() ->
    Results = gleam@erlang@process:new_subject(),
    Value = rx@future:pure(8),
    _ = rx@future:await(
        Value,
        fun(Result) -> gleam@erlang@process:send(Results, Result) end
    ),
    _pipe = gleam@erlang@process:'receive'(Results, 1000),
    gleeunit@should:equal(_pipe, {ok, {ok, 8}}).

-file("test/cookbook_test.gleam", 146).
-spec cookbook_14_future_to_stream_test() -> nil.
cookbook_14_future_to_stream_test() ->
    Value = rx@future:pure(11),
    _pipe = Value,
    _pipe@1 = rx@flow:from_future(_pipe),
    assert_stream(_pipe@1, [{value, 11}, completed]).

-file("test/cookbook_test.gleam", 153).
-spec cookbook_15_async_source_can_emit_later_test() -> nil.
cookbook_15_async_source_can_emit_later_test() ->
    Emitters = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_15_async_source_can_emit_later_test"/utf8>>,
                        line => 156,
                        value => _assert_fail,
                        start => 4564,
                        'end' => 4605,
                        pattern_start => 4575,
                        pattern_end => 4587})
    end,
    Source = rx:create(
        fun(Emitter) ->
            gleam@erlang@process:send(Emitters, Emitter),
            fun() -> nil end
        end
    ),
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_15_async_source_can_emit_later_test"/utf8>>,
                        line => 162,
                        value => _assert_fail@1,
                        start => 4743,
                        'end' => 4833,
                        pattern_start => 4754,
                        pattern_end => 4770})
    end,
    Emitter@2 = case gleam@erlang@process:'receive'(Emitters, 1000) of
        {ok, Emitter@1} -> Emitter@1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_15_async_source_can_emit_later_test"/utf8>>,
                        line => 164,
                        value => _assert_fail@2,
                        start => 4836,
                        'end' => 4906,
                        pattern_start => 4847,
                        pattern_end => 4858})
    end,
    _pipe = gleam@erlang@process:'receive'(Outputs, 20),
    gleeunit@should:equal(_pipe, {error, nil}),
    rx:next(Emitter@2, 21),
    rx:complete(Emitter@2),
    _pipe@1 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@1, {value, 21}),
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 318).
-spec controlled_future(
    integer(),
    gleam@erlang@process:subject(controlled_work()),
    gleam@erlang@process:subject(integer())
) -> rx@future:future(integer(), binary()).
controlled_future(Value, Work, Cancelled) ->
    rx@future:new(
        fun(Resolve) ->
            gleam@erlang@process:send(Work, {controlled_work, Value, Resolve}),
            fun() -> gleam@erlang@process:send(Cancelled, Value) end
        end
    ).

-file("test/cookbook_test.gleam", 176).
-spec cookbook_16_concat_map_is_fifo_test() -> nil.
cookbook_16_concat_map_is_fifo_test() ->
    Work = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_16_concat_map_is_fifo_test"/utf8>>,
                        line => 179,
                        value => _assert_fail,
                        start => 5315,
                        'end' => 5356,
                        pattern_start => 5326,
                        pattern_end => 5338})
    end,
    Source = begin
        _pipe = rx:from_list([1, 2, 3]),
        rx@flow:concat_map(
            _pipe,
            fun(Value) ->
                controlled_future(
                    Value,
                    Work,
                    gleam@erlang@process:new_subject()
                )
            end
        )
    end,
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_16_concat_map_is_fifo_test"/utf8>>,
                        line => 185,
                        value => _assert_fail@1,
                        start => 5532,
                        'end' => 5622,
                        pattern_start => 5543,
                        pattern_end => 5559})
    end,
    Resolve1@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 1, Resolve1}} -> Resolve1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_16_concat_map_is_fifo_test"/utf8>>,
                        line => 188,
                        value => _assert_fail@2,
                        start => 5626,
                        'end' => 5716,
                        pattern_start => 5637,
                        pattern_end => 5668})
    end,
    _pipe@1 = gleam@erlang@process:'receive'(Work, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    Resolve1@1({ok, 10}),
    Resolve2@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 2, Resolve2}} -> Resolve2;
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_16_concat_map_is_fifo_test"/utf8>>,
                        line => 193,
                        value => _assert_fail@3,
                        start => 5809,
                        'end' => 5899,
                        pattern_start => 5820,
                        pattern_end => 5851})
    end,
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {value, 10}),
    Resolve2@1({ok, 20}),
    Resolve3@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 3, Resolve3}} -> Resolve3;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_16_concat_map_is_fifo_test"/utf8>>,
                        line => 198,
                        value => _assert_fail@4,
                        start => 5975,
                        'end' => 6065,
                        pattern_start => 5986,
                        pattern_end => 6017})
    end,
    _pipe@3 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@3, {value, 20}),
    Resolve3@1({ok, 30}),
    _pipe@4 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@4, {value, 30}),
    _pipe@5 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@5, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 210).
-spec cookbook_17_merge_map_bounds_concurrency_test() -> nil.
cookbook_17_merge_map_bounds_concurrency_test() ->
    Work = gleam@erlang@process:new_subject(),
    Cancelled = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_17_merge_map_bounds_concurrency_test"/utf8>>,
                        line => 214,
                        value => _assert_fail,
                        start => 6477,
                        'end' => 6518,
                        pattern_start => 6488,
                        pattern_end => 6500})
    end,
    Source = begin
        _pipe = rx:from_list([1, 2, 3]),
        rx@flow:merge_map(
            _pipe,
            fun(Value) -> controlled_future(Value, Work, Cancelled) end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_17_merge_map_bounds_concurrency_test"/utf8>>,
                        line => 221,
                        value => _assert_fail@1,
                        start => 6693,
                        'end' => 6783,
                        pattern_start => 6704,
                        pattern_end => 6720})
    end,
    Resolve1@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 1, Resolve1}} -> Resolve1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_17_merge_map_bounds_concurrency_test"/utf8>>,
                        line => 224,
                        value => _assert_fail@2,
                        start => 6787,
                        'end' => 6877,
                        pattern_start => 6798,
                        pattern_end => 6829})
    end,
    Resolve2@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 2, Resolve2}} -> Resolve2;
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_17_merge_map_bounds_concurrency_test"/utf8>>,
                        line => 226,
                        value => _assert_fail@3,
                        start => 6880,
                        'end' => 6970,
                        pattern_start => 6891,
                        pattern_end => 6922})
    end,
    _pipe@1 = gleam@erlang@process:'receive'(Work, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    Resolve2@1({ok, 20}),
    Resolve3@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 3, Resolve3}} -> Resolve3;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_17_merge_map_bounds_concurrency_test"/utf8>>,
                        line => 231,
                        value => _assert_fail@4,
                        start => 7063,
                        'end' => 7153,
                        pattern_start => 7074,
                        pattern_end => 7105})
    end,
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {value, 20}),
    Resolve1@1({ok, 10}),
    _pipe@3 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@3, {value, 10}),
    Resolve3@1({ok, 30}),
    _pipe@4 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@4, {value, 30}),
    _pipe@5 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@5, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 245).
-spec cookbook_18_map_ordered_buffers_early_results_test() -> nil.
cookbook_18_map_ordered_buffers_early_results_test() ->
    Work = gleam@erlang@process:new_subject(),
    Cancelled = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_18_map_ordered_buffers_early_results_test"/utf8>>,
                        line => 249,
                        value => _assert_fail,
                        start => 7642,
                        'end' => 7683,
                        pattern_start => 7653,
                        pattern_end => 7665})
    end,
    Source = begin
        _pipe = rx:from_list([1, 2]),
        rx@flow:map_ordered(
            _pipe,
            fun(Value) -> controlled_future(Value, Work, Cancelled) end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_18_map_ordered_buffers_early_results_test"/utf8>>,
                        line => 256,
                        value => _assert_fail@1,
                        start => 7857,
                        'end' => 7947,
                        pattern_start => 7868,
                        pattern_end => 7884})
    end,
    Resolve1@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 1, Resolve1}} -> Resolve1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_18_map_ordered_buffers_early_results_test"/utf8>>,
                        line => 259,
                        value => _assert_fail@2,
                        start => 7951,
                        'end' => 8041,
                        pattern_start => 7962,
                        pattern_end => 7993})
    end,
    Resolve2@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 2, Resolve2}} -> Resolve2;
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_18_map_ordered_buffers_early_results_test"/utf8>>,
                        line => 261,
                        value => _assert_fail@3,
                        start => 8044,
                        'end' => 8134,
                        pattern_start => 8055,
                        pattern_end => 8086})
    end,
    Resolve2@1({ok, 20}),
    _pipe@1 = gleam@erlang@process:'receive'(Outputs, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    Resolve1@1({ok, 10}),
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {value, 10}),
    _pipe@3 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@3, {value, 20}),
    _pipe@4 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@4, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/cookbook_test.gleam", 275).
-spec cookbook_19_async_filter_preserves_order_test() -> nil.
cookbook_19_async_filter_preserves_order_test() ->
    Source = begin
        _pipe = rx:from_list([1, 2, 3, 4]),
        rx@flow:filter_async_concurrent(
            _pipe,
            fun(Value) -> rx@future:pure((Value rem 2) =:= 0) end,
            2
        )
    end,
    assert_stream(Source, [{value, 2}, {value, 4}, completed]).

-file("test/cookbook_test.gleam", 286).
-spec cookbook_20_fail_fast_cancels_and_ignores_late_result_test() -> nil.
cookbook_20_fail_fast_cancels_and_ignores_late_result_test() ->
    Work = gleam@erlang@process:new_subject(),
    Cancelled = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_20_fail_fast_cancels_and_ignores_late_result_test"/utf8>>,
                        line => 290,
                        value => _assert_fail,
                        start => 8944,
                        'end' => 8985,
                        pattern_start => 8955,
                        pattern_end => 8967})
    end,
    Source = begin
        _pipe = rx:from_list([1, 2, 3]),
        rx@flow:merge_map(
            _pipe,
            fun(Value) -> controlled_future(Value, Work, Cancelled) end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_20_fail_fast_cancels_and_ignores_late_result_test"/utf8>>,
                        line => 297,
                        value => _assert_fail@1,
                        start => 9160,
                        'end' => 9250,
                        pattern_start => 9171,
                        pattern_end => 9187})
    end,
    Resolve1@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 1, Resolve1}} -> Resolve1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_20_fail_fast_cancels_and_ignores_late_result_test"/utf8>>,
                        line => 300,
                        value => _assert_fail@2,
                        start => 9254,
                        'end' => 9344,
                        pattern_start => 9265,
                        pattern_end => 9296})
    end,
    Resolve2@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 2, Resolve2}} -> Resolve2;
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"cookbook_test"/utf8>>,
                        function => <<"cookbook_20_fail_fast_cancels_and_ignores_late_result_test"/utf8>>,
                        line => 302,
                        value => _assert_fail@3,
                        start => 9347,
                        'end' => 9437,
                        pattern_start => 9358,
                        pattern_end => 9389})
    end,
    _pipe@1 = gleam@erlang@process:'receive'(Work, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    Resolve2@1({error, <<"boom"/utf8>>}),
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {failed, <<"boom"/utf8>>}),
    _pipe@3 = gleam@erlang@process:'receive'(Cancelled, 1000),
    gleeunit@should:equal(_pipe@3, {ok, 1}),
    Resolve1@1({ok, 10}),
    _pipe@4 = gleam@erlang@process:'receive'(Outputs, 20),
    gleeunit@should:equal(_pipe@4, {error, nil}),
    _pipe@5 = gleam@erlang@process:'receive'(Work, 20),
    gleeunit@should:equal(_pipe@5, {error, nil}),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-module(eager_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/eager_test.gleam").
-export([main/0, eager_pipeline_is_immediate_and_composable_test/0, eager_scan_flat_map_and_fold_test/0, eager_error_short_circuits_mapping_test/0, eager_to_actor_backed_observable_bridge_test/0]).
-export_type([event/0]).

-type event() :: {value, integer()} | {failed, binary()} | completed.

-file("test/eager_test.gleam", 14).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/eager_test.gleam", 18).
-spec eager_pipeline_is_immediate_and_composable_test() -> nil.
eager_pipeline_is_immediate_and_composable_test() ->
    Sequence = begin
        _pipe = rx@eager:from_list([1, 2, 3, 4, 5]),
        _pipe@1 = rx@eager:map(_pipe, fun(Value) -> Value * 3 end),
        _pipe@2 = rx@eager:filter(_pipe@1, fun(Value@1) -> Value@1 > 6 end),
        rx@eager:take(_pipe@2, 2)
    end,
    _pipe@3 = Sequence,
    _pipe@4 = rx@eager:to_result(_pipe@3),
    gleeunit@should:equal(_pipe@4, {ok, [9, 12]}).

-file("test/eager_test.gleam", 30).
-spec eager_scan_flat_map_and_fold_test() -> nil.
eager_scan_flat_map_and_fold_test() ->
    Sequence = begin
        _pipe = rx@eager:from_list([1, 2, 3]),
        _pipe@1 = rx@eager:flat_map(
            _pipe,
            fun(Value) -> rx@eager:from_list([Value, Value * 10]) end
        ),
        rx@eager:scan(_pipe@1, 0, fun(Total, Value@1) -> Total + Value@1 end)
    end,
    _pipe@2 = Sequence,
    _pipe@3 = rx@eager:to_result(_pipe@2),
    gleeunit@should:equal(_pipe@3, {ok, [1, 11, 13, 33, 36, 66]}),
    _pipe@4 = Sequence,
    _pipe@5 = rx@eager:fold(
        _pipe@4,
        0,
        fun(Total@1, Value@2) -> Total@1 + Value@2 end
    ),
    gleeunit@should:equal(_pipe@5, {ok, 160}).

-file("test/eager_test.gleam", 45).
-spec eager_error_short_circuits_mapping_test() -> nil.
eager_error_short_circuits_mapping_test() ->
    Sequence = begin
        _pipe = rx@eager:fail(<<"boom"/utf8>>),
        _pipe@1 = rx@eager:map(_pipe, fun(Value) -> Value * 10 end),
        rx@eager:map_error(
            _pipe@1,
            fun(Reason) -> <<"wrapped:"/utf8, Reason/binary>> end
        )
    end,
    _pipe@2 = Sequence,
    _pipe@3 = rx@eager:to_result(_pipe@2),
    gleeunit@should:equal(_pipe@3, {error, <<"wrapped:boom"/utf8>>}).

-file("test/eager_test.gleam", 84).
-spec 'receive'(gleam@erlang@process:subject(event())) -> event().
'receive'(Events) ->
    Event@1 = case gleam@erlang@process:'receive'(Events, 1000) of
        {ok, Event} -> Event;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"eager_test"/utf8>>,
                        function => <<"receive"/utf8>>,
                        line => 85,
                        value => _assert_fail,
                        start => 2088,
                        'end' => 2154,
                        pattern_start => 2099,
                        pattern_end => 2108})
    end,
    Event@1.

-file("test/eager_test.gleam", 56).
-spec eager_to_actor_backed_observable_bridge_test() -> nil.
eager_to_actor_backed_observable_bridge_test() ->
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"eager_test"/utf8>>,
                        function => <<"eager_to_actor_backed_observable_bridge_test"/utf8>>,
                        line => 58,
                        value => _assert_fail,
                        start => 1351,
                        'end' => 1392,
                        pattern_start => 1362,
                        pattern_end => 1374})
    end,
    Stream = begin
        _pipe = rx@eager:from_list([2, 3, 4]),
        _pipe@1 = rx@eager:map(_pipe, fun(Value) -> Value * 10 end),
        _pipe@2 = rx@eager:to_observable(_pipe@1),
        rx:filter(_pipe@2, fun(Value@1) -> Value@1 >= 30 end)
    end,
    case rx:subscribe(
        Stream,
        Runtime_@1,
        rx:observer(
            fun(Value@2) ->
                gleam@erlang@process:send(Events, {value, Value@2})
            end,
            fun(Reason) ->
                gleam@erlang@process:send(Events, {failed, Reason})
            end,
            fun() -> gleam@erlang@process:send(Events, completed) end
        )
    ) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"eager_test"/utf8>>,
                        function => <<"eager_to_actor_backed_observable_bridge_test"/utf8>>,
                        line => 66,
                        value => _assert_fail@1,
                        start => 1585,
                        'end' => 1866,
                        pattern_start => 1596,
                        pattern_end => 1613})
    end,
    _pipe@3 = 'receive'(Events),
    gleeunit@should:equal(_pipe@3, {value, 30}),
    _pipe@4 = 'receive'(Events),
    gleeunit@should:equal(_pipe@4, {value, 40}),
    _pipe@5 = 'receive'(Events),
    gleeunit@should:equal(_pipe@5, completed),
    rx@runtime:stop(Runtime_@1).

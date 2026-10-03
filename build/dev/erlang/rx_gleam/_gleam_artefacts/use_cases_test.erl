-module(use_cases_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/use_cases_test.gleam").
-export([main/0, use_case_01_async_fifo_with_async_processing_test/0, use_case_02_deduplicate_requests_in_memory_test/0, use_case_03_group_requests_by_key_test/0, use_case_04_merge_server_push_sources_at_ingress_test/0, use_case_05_rebase_heterogeneous_inputs_test/0]).
-export_type([request/0, group_result/0, ingress_event/0, canonical_event/0, controlled_work/0, dedupe_message/0, group_message/0]).

-type request() :: {request, binary(), binary(), integer()}.

-type group_result() :: {group_result, binary(), integer()}.

-type ingress_event() :: {queue_message, binary()} | {socket_message, binary()}.

-type canonical_event() :: {user_changed, binary()} | {invoice_paid, binary()}.

-type controlled_work() :: {controlled_work,
        integer(),
        fun(({ok, integer()} | {error, binary()}) -> nil)}.

-type dedupe_message() :: {first_seen, binary(), fun((boolean()) -> nil)} |
    stop_dedupe.

-type group_message() :: {submit,
        request(),
        fun(({ok, group_result()} | {error, binary()}) -> nil)} |
    stop_groups.

-file("test/use_cases_test.gleam", 43).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/use_cases_test.gleam", 47).
-spec use_case_01_async_fifo_with_async_processing_test() -> nil.
use_case_01_async_fifo_with_async_processing_test() ->
    Emitters = gleam@erlang@process:new_subject(),
    Work = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_01_async_fifo_with_async_processing_test"/utf8>>,
                        line => 51,
                        value => _assert_fail,
                        start => 940,
                        'end' => 981,
                        pattern_start => 951,
                        pattern_end => 963})
    end,
    Source = rx:create(
        fun(Emitter) ->
            gleam@erlang@process:send(Emitters, Emitter),
            fun() -> nil end
        end
    ),
    Pipeline = begin
        _pipe = Source,
        rx@flow:concat_map(
            _pipe,
            fun(Value) ->
                rx@future:new(
                    fun(Resolve) ->
                        gleam@erlang@process:send(
                            Work,
                            {controlled_work, Value, Resolve}
                        ),
                        fun() -> nil end
                    end
                )
            end
        )
    end,
    Subscription@1 = case rx:subscribe(
        Pipeline,
        Runtime_@1,
        rx:observer(
            fun(Value@1) -> gleam@erlang@process:send(Outputs, Value@1) end,
            fun(_) -> nil end,
            fun() -> nil end
        )
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_01_async_fifo_with_async_processing_test"/utf8>>,
                        line => 65,
                        value => _assert_fail@1,
                        start => 1309,
                        'end' => 1519,
                        pattern_start => 1320,
                        pattern_end => 1336})
    end,
    Emitter@2 = case gleam@erlang@process:'receive'(Emitters, 1000) of
        {ok, Emitter@1} -> Emitter@1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_01_async_fifo_with_async_processing_test"/utf8>>,
                        line => 75,
                        value => _assert_fail@2,
                        start => 1522,
                        'end' => 1592,
                        pattern_start => 1533,
                        pattern_end => 1544})
    end,
    rx:next(Emitter@2, 1),
    rx:next(Emitter@2, 2),
    Resolve1@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 1, Resolve1}} -> Resolve1;
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_01_async_fifo_with_async_processing_test"/utf8>>,
                        line => 79,
                        value => _assert_fail@3,
                        start => 1640,
                        'end' => 1730,
                        pattern_start => 1651,
                        pattern_end => 1682})
    end,
    _pipe@1 = gleam@erlang@process:'receive'(Work, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    Resolve1@1({ok, 10}),
    Resolve2@1 = case gleam@erlang@process:'receive'(Work, 1000) of
        {ok, {controlled_work, 2, Resolve2}} -> Resolve2;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_01_async_fifo_with_async_processing_test"/utf8>>,
                        line => 84,
                        value => _assert_fail@4,
                        start => 1823,
                        'end' => 1913,
                        pattern_start => 1834,
                        pattern_end => 1865})
    end,
    _pipe@2 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@2, {ok, 10}),
    Resolve2@1({ok, 20}),
    _pipe@3 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@3, {ok, 20}),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/use_cases_test.gleam", 272).
-spec first_seen(gleam@erlang@process:subject(dedupe_message()), binary()) -> rx@future:future(boolean(), binary()).
first_seen(Dedupe, Id) ->
    rx@future:new(
        fun(Resolve) ->
            gleam@erlang@process:send(
                Dedupe,
                {first_seen, Id, fun(Value) -> Resolve({ok, Value}) end}
            ),
            fun() -> nil end
        end
    ).

-file("test/use_cases_test.gleam", 252).
-spec handle_dedupe(gleam@dict:dict(binary(), nil), dedupe_message()) -> gleam@otp@actor:next(gleam@dict:dict(binary(), nil), dedupe_message()).
handle_dedupe(State, Message) ->
    case Message of
        stop_dedupe ->
            gleam@otp@actor:stop();

        {first_seen, Id, Resolve} ->
            case gleam_stdlib:map_get(State, Id) of
                {ok, _} ->
                    Resolve(false),
                    gleam@otp@actor:continue(State);

                {error, _} ->
                    Resolve(true),
                    gleam@otp@actor:continue(gleam@dict:insert(State, Id, nil))
            end
    end.

-file("test/use_cases_test.gleam", 243).
-spec start_dedupe() -> gleam@erlang@process:subject(dedupe_message()).
start_dedupe() ->
    Started@1 = case begin
        _pipe = gleam@otp@actor:new(maps:new()),
        _pipe@1 = gleam@otp@actor:on_message(_pipe, fun handle_dedupe/2),
        gleam@otp@actor:start(_pipe@1)
    end of
        {ok, Started} -> Started;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"start_dedupe"/utf8>>,
                        line => 244,
                        value => _assert_fail,
                        start => 6722,
                        'end' => 6830,
                        pattern_start => 6733,
                        pattern_end => 6744})
    end,
    {started, _, Subject} = Started@1,
    Subject.

-file("test/use_cases_test.gleam", 95).
-spec use_case_02_deduplicate_requests_in_memory_test() -> nil.
use_case_02_deduplicate_requests_in_memory_test() ->
    Dedupe = start_dedupe(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_02_deduplicate_requests_in_memory_test"/utf8>>,
                        line => 98,
                        value => _assert_fail,
                        start => 2265,
                        'end' => 2306,
                        pattern_start => 2276,
                        pattern_end => 2288})
    end,
    Requests = rx:from_list(
        [{request, <<"a"/utf8>>, <<"t1"/utf8>>, 1},
            {request, <<"a"/utf8>>, <<"t1"/utf8>>, 2},
            {request, <<"b"/utf8>>, <<"t1"/utf8>>, 3}]
    ),
    Unique = begin
        _pipe = Requests,
        rx@flow:filter_async_concurrent(
            _pipe,
            fun(Request) ->
                {request, Id, _, _} = Request,
                first_seen(Dedupe, Id)
            end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Unique,
        Runtime_@1,
        rx:observer(
            fun(Request@1) -> gleam@erlang@process:send(Outputs, Request@1) end,
            fun(_) -> nil end,
            fun() -> nil end
        )
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_02_deduplicate_requests_in_memory_test"/utf8>>,
                        line => 114,
                        value => _assert_fail@1,
                        start => 2736,
                        'end' => 2948,
                        pattern_start => 2747,
                        pattern_end => 2763})
    end,
    _pipe@1 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(
        _pipe@1,
        {ok, {request, <<"a"/utf8>>, <<"t1"/utf8>>, 1}}
    ),
    _pipe@2 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(
        _pipe@2,
        {ok, {request, <<"b"/utf8>>, <<"t1"/utf8>>, 3}}
    ),
    _pipe@3 = gleam@erlang@process:'receive'(Outputs, 20),
    gleeunit@should:equal(_pipe@3, {error, nil}),
    rx:unsubscribe(Subscription@1),
    gleam@erlang@process:send(Dedupe, stop_dedupe),
    rx@runtime:stop(Runtime_@1).

-file("test/use_cases_test.gleam", 317).
-spec group_submit(gleam@erlang@process:subject(group_message()), request()) -> rx@future:future(group_result(), binary()).
group_submit(Groups, Request) ->
    rx@future:new(
        fun(Resolve) ->
            gleam@erlang@process:send(Groups, {submit, Request, Resolve}),
            fun() -> nil end
        end
    ).

-file("test/use_cases_test.gleam", 291).
-spec handle_groups(gleam@dict:dict(binary(), integer()), group_message()) -> gleam@otp@actor:next(gleam@dict:dict(binary(), integer()), group_message()).
handle_groups(State, Message) ->
    case Message of
        stop_groups ->
            gleam@otp@actor:stop();

        {submit, Request, Resolve} ->
            {request, _, Tenant, Sequence} = Request,
            Expected = case gleam_stdlib:map_get(State, Tenant) of
                {ok, Last} ->
                    Last + 1;

                {error, _} ->
                    1
            end,
            case Sequence =:= Expected of
                true ->
                    Resolve({ok, {group_result, Tenant, Sequence}}),
                    gleam@otp@actor:continue(
                        gleam@dict:insert(State, Tenant, Sequence)
                    );

                false ->
                    Resolve({error, <<"out-of-order group request"/utf8>>}),
                    gleam@otp@actor:continue(State)
            end
    end.

-file("test/use_cases_test.gleam", 282).
-spec start_groups() -> gleam@erlang@process:subject(group_message()).
start_groups() ->
    Started@1 = case begin
        _pipe = gleam@otp@actor:new(maps:new()),
        _pipe@1 = gleam@otp@actor:on_message(_pipe, fun handle_groups/2),
        gleam@otp@actor:start(_pipe@1)
    end of
        {ok, Started} -> Started;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"start_groups"/utf8>>,
                        line => 283,
                        value => _assert_fail,
                        start => 7630,
                        'end' => 7738,
                        pattern_start => 7641,
                        pattern_end => 7652})
    end,
    {started, _, Subject} = Started@1,
    Subject.

-file("test/use_cases_test.gleam", 136).
-spec use_case_03_group_requests_by_key_test() -> nil.
use_case_03_group_requests_by_key_test() ->
    Groups = start_groups(),
    Outputs = gleam@erlang@process:new_subject(),
    Errors = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_03_group_requests_by_key_test"/utf8>>,
                        line => 140,
                        value => _assert_fail,
                        start => 3503,
                        'end' => 3544,
                        pattern_start => 3514,
                        pattern_end => 3526})
    end,
    Requests = rx:from_list(
        [{request, <<"a1"/utf8>>, <<"a"/utf8>>, 1},
            {request, <<"b1"/utf8>>, <<"b"/utf8>>, 1},
            {request, <<"a2"/utf8>>, <<"a"/utf8>>, 2},
            {request, <<"b2"/utf8>>, <<"b"/utf8>>, 2}]
    ),
    Grouped = begin
        _pipe = Requests,
        rx@flow:merge_map(
            _pipe,
            fun(Request) -> group_submit(Groups, Request) end,
            4
        )
    end,
    Subscription@1 = case rx:subscribe(
        Grouped,
        Runtime_@1,
        rx:observer(
            fun(Result) -> gleam@erlang@process:send(Outputs, Result) end,
            fun(Reason) -> gleam@erlang@process:send(Errors, Reason) end,
            fun() -> nil end
        )
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_03_group_requests_by_key_test"/utf8>>,
                        line => 151,
                        value => _assert_fail@1,
                        start => 3927,
                        'end' => 4168,
                        pattern_start => 3938,
                        pattern_end => 3954})
    end,
    _pipe@1 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@1, {ok, {group_result, <<"a"/utf8>>, 1}}),
    _pipe@2 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@2, {ok, {group_result, <<"b"/utf8>>, 1}}),
    _pipe@3 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@3, {ok, {group_result, <<"a"/utf8>>, 2}}),
    _pipe@4 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@4, {ok, {group_result, <<"b"/utf8>>, 2}}),
    _pipe@5 = gleam@erlang@process:'receive'(Errors, 20),
    gleeunit@should:equal(_pipe@5, {error, nil}),
    rx:unsubscribe(Subscription@1),
    gleam@erlang@process:send(Groups, stop_groups),
    rx@runtime:stop(Runtime_@1).

-file("test/use_cases_test.gleam", 177).
-spec use_case_04_merge_server_push_sources_at_ingress_test() -> nil.
use_case_04_merge_server_push_sources_at_ingress_test() ->
    Emitters = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_04_merge_server_push_sources_at_ingress_test"/utf8>>,
                        line => 180,
                        value => _assert_fail,
                        start => 4913,
                        'end' => 4954,
                        pattern_start => 4924,
                        pattern_end => 4936})
    end,
    Merged = rx:create(
        fun(Emitter) ->
            gleam@erlang@process:send(Emitters, Emitter),
            fun() -> nil end
        end
    ),
    Subscription@1 = case rx:subscribe(
        Merged,
        Runtime_@1,
        rx:observer(
            fun(Event) -> gleam@erlang@process:send(Outputs, Event) end,
            fun(_) -> nil end,
            fun() -> nil end
        )
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_04_merge_server_push_sources_at_ingress_test"/utf8>>,
                        line => 186,
                        value => _assert_fail@1,
                        start => 5101,
                        'end' => 5309,
                        pattern_start => 5112,
                        pattern_end => 5128})
    end,
    Emitter@2 = case gleam@erlang@process:'receive'(Emitters, 1000) of
        {ok, Emitter@1} -> Emitter@1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_04_merge_server_push_sources_at_ingress_test"/utf8>>,
                        line => 196,
                        value => _assert_fail@2,
                        start => 5312,
                        'end' => 5382,
                        pattern_start => 5323,
                        pattern_end => 5334})
    end,
    rx:next(Emitter@2, {queue_message, <<"job-1"/utf8>>}),
    rx:next(Emitter@2, {socket_message, <<"peer-1"/utf8>>}),
    _pipe = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe, {ok, {queue_message, <<"job-1"/utf8>>}}),
    _pipe@1 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@1, {ok, {socket_message, <<"peer-1"/utf8>>}}),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/use_cases_test.gleam", 210).
-spec use_case_05_rebase_heterogeneous_inputs_test() -> nil.
use_case_05_rebase_heterogeneous_inputs_test() ->
    Emitters = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_05_rebase_heterogeneous_inputs_test"/utf8>>,
                        line => 213,
                        value => _assert_fail,
                        start => 5852,
                        'end' => 5893,
                        pattern_start => 5863,
                        pattern_end => 5875})
    end,
    Canonical = rx:create(
        fun(Emitter) ->
            gleam@erlang@process:send(Emitters, Emitter),
            fun() -> nil end
        end
    ),
    Subscription@1 = case rx:subscribe(
        Canonical,
        Runtime_@1,
        rx:observer(
            fun(Event) -> gleam@erlang@process:send(Outputs, Event) end,
            fun(_) -> nil end,
            fun() -> nil end
        )
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_05_rebase_heterogeneous_inputs_test"/utf8>>,
                        line => 219,
                        value => _assert_fail@1,
                        start => 6045,
                        'end' => 6256,
                        pattern_start => 6056,
                        pattern_end => 6072})
    end,
    Emitter@2 = case gleam@erlang@process:'receive'(Emitters, 1000) of
        {ok, Emitter@1} -> Emitter@1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"use_cases_test"/utf8>>,
                        function => <<"use_case_05_rebase_heterogeneous_inputs_test"/utf8>>,
                        line => 229,
                        value => _assert_fail@2,
                        start => 6259,
                        'end' => 6329,
                        pattern_start => 6270,
                        pattern_end => 6281})
    end,
    rx:next(Emitter@2, {user_changed, <<"user-7"/utf8>>}),
    rx:next(Emitter@2, {invoice_paid, <<"invoice-9"/utf8>>}),
    _pipe = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe, {ok, {user_changed, <<"user-7"/utf8>>}}),
    _pipe@1 = gleam@erlang@process:'receive'(Outputs, 1000),
    gleeunit@should:equal(_pipe@1, {ok, {invoice_paid, <<"invoice-9"/utf8>>}}),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

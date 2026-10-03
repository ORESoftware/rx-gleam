-module(rx@runtime).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/runtime.gleam").
-export([start_checked/1, start/0, register/1, set_teardown/3, dispatch/4, cancel/2, register_flow/4, enqueue_flow/3, complete_flow/5, finish_flow_input/2, fail_flow_input/3, cancel_flow/2, stop/1]).
-export_type([runtime/0, subscription_key/0, flow_key/0, flow_order/0, flow_completion/0, runtime_error/0, runtime_diagnostic/0, entry/0, fifo/1, pending_work/0, active_work/0, completed_work/0, flow_entry/0, state/0, message/0]).

-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-opaque runtime() :: {runtime,
        gleam@erlang@process:pid_(),
        gleam@erlang@process:subject(message())}.

-opaque subscription_key() :: {subscription_key,
        gleam@erlang@reference:reference_()}.

-opaque flow_key() :: {flow_key, gleam@erlang@reference:reference_()}.

-type flow_order() :: completion_order | input_order.

-type flow_completion() :: flow_success | flow_failure.

-type runtime_error() :: runtime_stopped | invalid_concurrency.

-type runtime_diagnostic() :: {protocol_violation, rx@protocol:protocol_error()} |
    duplicate_teardown_registration.

-type entry() :: {entry,
        rx@protocol:phase(),
        gleam@option:option(fun(() -> nil))}.

-type fifo(FNR) :: {fifo, list(FNR), list(FNR)}.

-type pending_work() :: {pending_work,
        integer(),
        fun((integer()) -> fun(() -> nil))}.

-type active_work() :: {active_work, integer(), fun(() -> nil)}.

-type completed_work() :: {completed_work, integer(), fun(() -> nil)}.

-type flow_entry() :: {flow_entry,
        integer(),
        flow_order(),
        integer(),
        integer(),
        fifo(pending_work()),
        list(active_work()),
        integer(),
        list(completed_work()),
        boolean(),
        fun(() -> nil)}.

-type state() :: {state,
        gleam@dict:dict(gleam@erlang@reference:reference_(), entry()),
        gleam@dict:dict(gleam@erlang@reference:reference_(), flow_entry()),
        fun((runtime_diagnostic()) -> nil)}.

-type message() :: {register, gleam@erlang@reference:reference_()} |
    {set_teardown, gleam@erlang@reference:reference_(), fun(() -> nil)} |
    {notify,
        gleam@erlang@reference:reference_(),
        rx@protocol:kind(),
        fun(() -> nil)} |
    {cancel, gleam@erlang@reference:reference_()} |
    {register_flow,
        gleam@erlang@reference:reference_(),
        integer(),
        flow_order(),
        fun(() -> nil)} |
    {enqueue_flow,
        gleam@erlang@reference:reference_(),
        fun((integer()) -> fun(() -> nil))} |
    {complete_flow,
        gleam@erlang@reference:reference_(),
        integer(),
        flow_completion(),
        fun(() -> nil)} |
    {finish_flow_input, gleam@erlang@reference:reference_()} |
    {fail_flow_input, gleam@erlang@reference:reference_(), fun(() -> nil)} |
    {cancel_flow, gleam@erlang@reference:reference_()} |
    stop.

-file("src/rx/runtime.gleam", 573).
-spec cancel_active(list(active_work())) -> nil.
cancel_active(Active) ->
    case Active of
        [] ->
            nil;

        [First | Rest] ->
            (erlang:element(3, First))(),
            cancel_active(Rest)
    end.

-file("src/rx/runtime.gleam", 587).
-spec cancel_all_flows(
    gleam@dict:dict(gleam@erlang@reference:reference_(), flow_entry())
) -> nil.
cancel_all_flows(Flows) ->
    gleam@dict:each(
        Flows,
        fun(_, Flow) -> cancel_active(erlang:element(7, Flow)) end
    ).

-file("src/rx/runtime.gleam", 677).
-spec run_optional(gleam@option:option(fun(() -> nil))) -> nil.
run_optional(Callback) ->
    case Callback of
        {some, Run} ->
            Run();

        none ->
            nil
    end.

-file("src/rx/runtime.gleam", 583).
-spec cancel_all_entries(
    gleam@dict:dict(gleam@erlang@reference:reference_(), entry())
) -> nil.
cancel_all_entries(Entries) ->
    gleam@dict:each(
        Entries,
        fun(_, Entry) -> run_optional(erlang:element(3, Entry)) end
    ).

-file("src/rx/runtime.gleam", 617).
-spec list_is_empty(list(any())) -> boolean().
list_is_empty(Items) ->
    case Items of
        [] ->
            true;

        _ ->
            false
    end.

-file("src/rx/runtime.gleam", 610).
-spec fifo_is_empty(fifo(any())) -> boolean().
fifo_is_empty(Fifo) ->
    case {erlang:element(2, Fifo), erlang:element(3, Fifo)} of
        {[], []} ->
            true;

        {_, _} ->
            false
    end.

-file("src/rx/runtime.gleam", 531).
-spec flow_is_drained(flow_entry()) -> boolean().
flow_is_drained(Flow) ->
    ((erlang:element(10, Flow) andalso (erlang:element(8, Flow) =:= 0)) andalso fifo_is_empty(
        erlang:element(6, Flow)
    ))
    andalso list_is_empty(erlang:element(9, Flow)).

-file("src/rx/runtime.gleam", 480).
-spec continue_flow(state(), gleam@erlang@reference:reference_(), flow_entry()) -> gleam@otp@actor:next(state(), message()).
continue_flow(State, Id, Flow) ->
    case flow_is_drained(Flow) of
        true ->
            (erlang:element(11, Flow))(),
            gleam@otp@actor:continue(
                {state,
                    erlang:element(2, State),
                    gleam@dict:delete(erlang:element(3, State), Id),
                    erlang:element(4, State)}
            );

        false ->
            gleam@otp@actor:continue(
                {state,
                    erlang:element(2, State),
                    gleam@dict:insert(erlang:element(3, State), Id, Flow),
                    erlang:element(4, State)}
            )
    end.

-file("src/rx/runtime.gleam", 599).
-spec fifo_pop(fifo(FOU)) -> {ok, {FOU, fifo(FOU)}} | {error, nil}.
fifo_pop(Fifo) ->
    case erlang:element(2, Fifo) of
        [First | Rest] ->
            {ok, {First, {fifo, Rest, erlang:element(3, Fifo)}}};

        [] ->
            case lists:reverse(erlang:element(3, Fifo)) of
                [] ->
                    {error, nil};

                [First@1 | Rest@1] ->
                    {ok, {First@1, {fifo, Rest@1, []}}}
            end
    end.

-file("src/rx/runtime.gleam", 495).
-spec fill_slots(flow_entry()) -> flow_entry().
fill_slots(Flow) ->
    case erlang:element(8, Flow) < erlang:element(2, Flow) of
        false ->
            Flow;

        true ->
            case fifo_pop(erlang:element(6, Flow)) of
                {error, _} ->
                    Flow;

                {ok, {{pending_work, Sequence, Start_work}, Remaining}} ->
                    Cancel_work = Start_work(Sequence),
                    fill_slots(
                        {flow_entry,
                            erlang:element(2, Flow),
                            erlang:element(3, Flow),
                            erlang:element(4, Flow),
                            erlang:element(5, Flow),
                            Remaining,
                            [{active_work, Sequence, Cancel_work} |
                                erlang:element(7, Flow)],
                            erlang:element(8, Flow) + 1,
                            erlang:element(9, Flow),
                            erlang:element(10, Flow),
                            erlang:element(11, Flow)}
                    )
            end
    end.

-file("src/rx/runtime.gleam", 555).
-spec take_completed(list(completed_work()), integer()) -> {ok,
        {fun(() -> nil), list(completed_work())}} |
    {error, nil}.
take_completed(Completed, Sequence) ->
    case Completed of
        [] ->
            {error, nil};

        [Item | Rest] ->
            case erlang:element(2, Item) =:= Sequence of
                true ->
                    {ok, {erlang:element(3, Item), Rest}};

                false ->
                    case take_completed(Rest, Sequence) of
                        {error, _} ->
                            {error, nil};

                        {ok, {Deliver, Remaining}} ->
                            {ok, {Deliver, [Item | Remaining]}}
                    end
            end
    end.

-file("src/rx/runtime.gleam", 519).
-spec flush_ordered(flow_entry()) -> flow_entry().
flush_ordered(Flow) ->
    case take_completed(erlang:element(9, Flow), erlang:element(5, Flow)) of
        {error, _} ->
            Flow;

        {ok, {Deliver, Remaining}} ->
            Deliver(),
            flush_ordered(
                {flow_entry,
                    erlang:element(2, Flow),
                    erlang:element(3, Flow),
                    erlang:element(4, Flow),
                    erlang:element(5, Flow) + 1,
                    erlang:element(6, Flow),
                    erlang:element(7, Flow),
                    erlang:element(8, Flow),
                    Remaining,
                    erlang:element(10, Flow),
                    erlang:element(11, Flow)}
            )
    end.

-file("src/rx/runtime.gleam", 538).
-spec remove_active(list(active_work()), integer()) -> {boolean(),
    list(active_work())}.
remove_active(Active, Sequence) ->
    case Active of
        [] ->
            {false, []};

        [Item | Rest] ->
            case erlang:element(2, Item) =:= Sequence of
                true ->
                    {true, Rest};

                false ->
                    {Found, Remaining} = remove_active(Rest, Sequence),
                    {Found, [Item | Remaining]}
            end
    end.

-file("src/rx/runtime.gleam", 595).
-spec fifo_push(fifo(FOR), FOR) -> fifo(FOR).
fifo_push(Fifo, Item) ->
    {fifo, erlang:element(2, Fifo), [Item | erlang:element(3, Fifo)]}.

-file("src/rx/runtime.gleam", 591).
-spec fifo_new() -> fifo(any()).
fifo_new() ->
    {fifo, [], []}.

-file("src/rx/runtime.gleam", 632).
-spec update_entry(
    gleam@dict:dict(gleam@erlang@reference:reference_(), entry()),
    gleam@erlang@reference:reference_(),
    rx@lifecycle:state(),
    gleam@option:option(fun(() -> nil)),
    gleam@option:option(fun(() -> nil))
) -> gleam@dict:dict(gleam@erlang@reference:reference_(), entry()).
update_entry(Entries, Id, Next_state, Stored, Incoming) ->
    case Next_state of
        closed ->
            gleam@dict:delete(Entries, Id);

        {active, Phase, Teardown_ready} ->
            Teardown = case {Teardown_ready, Stored, Incoming} of
                {false, _, _} ->
                    none;

                {true, {some, Existing}, _} ->
                    {some, Existing};

                {true, none, {some, New_teardown}} ->
                    {some, New_teardown};

                {true, none, none} ->
                    none
            end,
            gleam@dict:insert(Entries, Id, {entry, Phase, Teardown})
    end.

-file("src/rx/runtime.gleam", 653).
-spec run_commands(
    list(rx@lifecycle:command()),
    fun(() -> nil),
    gleam@option:option(fun(() -> nil)),
    gleam@option:option(fun(() -> nil)),
    fun((runtime_diagnostic()) -> nil)
) -> nil.
run_commands(Commands, Deliver, Stored, Incoming, On_diagnostic) ->
    case Commands of
        [] ->
            nil;

        [Command | Rest] ->
            case Command of
                deliver ->
                    Deliver();

                run_stored_teardown ->
                    run_optional(Stored);

                run_incoming_teardown ->
                    run_optional(Incoming);

                {report_protocol_error, Reason} ->
                    On_diagnostic({protocol_violation, Reason});

                report_duplicate_teardown ->
                    On_diagnostic(duplicate_teardown_registration)
            end,
            run_commands(Rest, Deliver, Stored, Incoming, On_diagnostic)
    end.

-file("src/rx/runtime.gleam", 624).
-spec lifecycle_state(entry()) -> rx@lifecycle:state().
lifecycle_state(Entry) ->
    {entry, Phase, Teardown} = Entry,
    {active, Phase, case Teardown of
            {some, _} ->
                true;

            none ->
                false
        end}.

-file("src/rx/runtime.gleam", 262).
-spec handle_message(state(), message()) -> gleam@otp@actor:next(state(), message()).
handle_message(State, Message) ->
    case Message of
        {register, Id} ->
            gleam@otp@actor:continue(
                {state,
                    gleam@dict:insert(
                        erlang:element(2, State),
                        Id,
                        {entry, open, none}
                    ),
                    erlang:element(3, State),
                    erlang:element(4, State)}
            );

        {set_teardown, Id@1, Incoming} ->
            Current = gleam_stdlib:map_get(erlang:element(2, State), Id@1),
            Model_state = case Current of
                {error, _} ->
                    closed;

                {ok, Entry} ->
                    lifecycle_state(Entry)
            end,
            {Next_state, Commands} = rx@lifecycle:transition(
                Model_state,
                install_teardown
            ),
            Stored = case Current of
                {ok, {entry, _, Teardown}} ->
                    Teardown;

                {error, _} ->
                    none
            end,
            run_commands(
                Commands,
                fun() -> nil end,
                Stored,
                {some, Incoming},
                erlang:element(4, State)
            ),
            gleam@otp@actor:continue(
                {state,
                    update_entry(
                        erlang:element(2, State),
                        Id@1,
                        Next_state,
                        Stored,
                        {some, Incoming}
                    ),
                    erlang:element(3, State),
                    erlang:element(4, State)}
            );

        {notify, Id@2, Kind, Work} ->
            case gleam_stdlib:map_get(erlang:element(2, State), Id@2) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Entry@1} ->
                    {entry, _, Stored@1} = Entry@1,
                    {Next_state@1, Commands@1} = rx@lifecycle:transition(
                        lifecycle_state(Entry@1),
                        {notify, Kind}
                    ),
                    run_commands(
                        Commands@1,
                        Work,
                        Stored@1,
                        none,
                        erlang:element(4, State)
                    ),
                    gleam@otp@actor:continue(
                        {state,
                            update_entry(
                                erlang:element(2, State),
                                Id@2,
                                Next_state@1,
                                Stored@1,
                                none
                            ),
                            erlang:element(3, State),
                            erlang:element(4, State)}
                    )
            end;

        {cancel, Id@3} ->
            case gleam_stdlib:map_get(erlang:element(2, State), Id@3) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Entry@2} ->
                    {entry, _, Stored@2} = Entry@2,
                    {Next_state@2, Commands@2} = rx@lifecycle:transition(
                        lifecycle_state(Entry@2),
                        cancel
                    ),
                    run_commands(
                        Commands@2,
                        fun() -> nil end,
                        Stored@2,
                        none,
                        erlang:element(4, State)
                    ),
                    gleam@otp@actor:continue(
                        {state,
                            update_entry(
                                erlang:element(2, State),
                                Id@3,
                                Next_state@2,
                                Stored@2,
                                none
                            ),
                            erlang:element(3, State),
                            erlang:element(4, State)}
                    )
            end;

        {register_flow, Id@4, Concurrency, Order, On_drain} ->
            gleam@otp@actor:continue(
                {state,
                    erlang:element(2, State),
                    gleam@dict:insert(
                        erlang:element(3, State),
                        Id@4,
                        {flow_entry,
                            Concurrency,
                            Order,
                            0,
                            0,
                            fifo_new(),
                            [],
                            0,
                            [],
                            false,
                            On_drain}
                    ),
                    erlang:element(4, State)}
            );

        {enqueue_flow, Id@5, Start_work} ->
            case gleam_stdlib:map_get(erlang:element(3, State), Id@5) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Flow} ->
                    Sequence = erlang:element(4, Flow),
                    Queued = {flow_entry,
                        erlang:element(2, Flow),
                        erlang:element(3, Flow),
                        Sequence + 1,
                        erlang:element(5, Flow),
                        fifo_push(
                            erlang:element(6, Flow),
                            {pending_work, Sequence, Start_work}
                        ),
                        erlang:element(7, Flow),
                        erlang:element(8, Flow),
                        erlang:element(9, Flow),
                        erlang:element(10, Flow),
                        erlang:element(11, Flow)},
                    Running = fill_slots(Queued),
                    gleam@otp@actor:continue(
                        {state,
                            erlang:element(2, State),
                            gleam@dict:insert(
                                erlang:element(3, State),
                                Id@5,
                                Running
                            ),
                            erlang:element(4, State)}
                    )
            end;

        {complete_flow, Id@6, Sequence@1, Completion, Deliver} ->
            case gleam_stdlib:map_get(erlang:element(3, State), Id@6) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Flow@1} ->
                    case remove_active(erlang:element(7, Flow@1), Sequence@1) of
                        {false, _} ->
                            gleam@otp@actor:continue(State);

                        {true, Remaining_active} ->
                            Without_active = {flow_entry,
                                erlang:element(2, Flow@1),
                                erlang:element(3, Flow@1),
                                erlang:element(4, Flow@1),
                                erlang:element(5, Flow@1),
                                erlang:element(6, Flow@1),
                                Remaining_active,
                                erlang:element(8, Flow@1) - 1,
                                erlang:element(9, Flow@1),
                                erlang:element(10, Flow@1),
                                erlang:element(11, Flow@1)},
                            case Completion of
                                flow_failure ->
                                    Deliver(),
                                    cancel_active(Remaining_active),
                                    gleam@otp@actor:continue(
                                        {state,
                                            erlang:element(2, State),
                                            gleam@dict:delete(
                                                erlang:element(3, State),
                                                Id@6
                                            ),
                                            erlang:element(4, State)}
                                    );

                                flow_success ->
                                    Emitted = case erlang:element(3, Flow@1) of
                                        completion_order ->
                                            Deliver(),
                                            Without_active;

                                        input_order ->
                                            flush_ordered(
                                                {flow_entry,
                                                    erlang:element(
                                                        2,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        3,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        4,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        5,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        6,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        7,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        8,
                                                        Without_active
                                                    ),
                                                    [{completed_work,
                                                            Sequence@1,
                                                            Deliver} |
                                                        erlang:element(
                                                            9,
                                                            Without_active
                                                        )],
                                                    erlang:element(
                                                        10,
                                                        Without_active
                                                    ),
                                                    erlang:element(
                                                        11,
                                                        Without_active
                                                    )}
                                            )
                                    end,
                                    Refilled = fill_slots(Emitted),
                                    continue_flow(State, Id@6, Refilled)
                            end
                    end
            end;

        {finish_flow_input, Id@7} ->
            case gleam_stdlib:map_get(erlang:element(3, State), Id@7) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Flow@2} ->
                    continue_flow(
                        State,
                        Id@7,
                        {flow_entry,
                            erlang:element(2, Flow@2),
                            erlang:element(3, Flow@2),
                            erlang:element(4, Flow@2),
                            erlang:element(5, Flow@2),
                            erlang:element(6, Flow@2),
                            erlang:element(7, Flow@2),
                            erlang:element(8, Flow@2),
                            erlang:element(9, Flow@2),
                            true,
                            erlang:element(11, Flow@2)}
                    )
            end;

        {fail_flow_input, Id@8, Deliver_error} ->
            case gleam_stdlib:map_get(erlang:element(3, State), Id@8) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Flow@3} ->
                    Deliver_error(),
                    cancel_active(erlang:element(7, Flow@3)),
                    gleam@otp@actor:continue(
                        {state,
                            erlang:element(2, State),
                            gleam@dict:delete(erlang:element(3, State), Id@8),
                            erlang:element(4, State)}
                    )
            end;

        {cancel_flow, Id@9} ->
            case gleam_stdlib:map_get(erlang:element(3, State), Id@9) of
                {error, _} ->
                    gleam@otp@actor:continue(State);

                {ok, Flow@4} ->
                    cancel_active(erlang:element(7, Flow@4)),
                    gleam@otp@actor:continue(
                        {state,
                            erlang:element(2, State),
                            gleam@dict:delete(erlang:element(3, State), Id@9),
                            erlang:element(4, State)}
                    )
            end;

        stop ->
            cancel_all_entries(erlang:element(2, State)),
            cancel_all_flows(erlang:element(3, State)),
            gleam@otp@actor:stop()
    end.

-file("src/rx/runtime.gleam", 109).
-spec start_checked(fun((runtime_diagnostic()) -> nil)) -> {ok, runtime()} |
    {error, gleam@otp@actor:start_error()}.
start_checked(On_diagnostic) ->
    _pipe = gleam@otp@actor:new({state, maps:new(), maps:new(), On_diagnostic}),
    _pipe@1 = gleam@otp@actor:on_message(_pipe, fun handle_message/2),
    _pipe@2 = gleam@otp@actor:start(_pipe@1),
    gleam@result:map(
        _pipe@2,
        fun(Started) ->
            {started, Pid, Subject} = Started,
            {runtime, Pid, Subject}
        end
    ).

-file("src/rx/runtime.gleam", 105).
-spec start() -> {ok, runtime()} | {error, gleam@otp@actor:start_error()}.
start() ->
    start_checked(fun(_) -> nil end).

-file("src/rx/runtime.gleam", 127).
?DOC(
    " Reserve one subscription entry without blocking on the runtime actor.\n"
    "\n"
    " Registration is deliberately one-way so observer callbacks can subscribe to\n"
    " additional streams on the same runtime without self-deadlocking. The\n"
    " `Register` message is sent before producer code can emit or spawn work, so\n"
    " subsequent messages from that subscription are ordered behind registration.\n"
).
-spec register(runtime()) -> {ok, subscription_key()} | {error, runtime_error()}.
register(Runtime) ->
    {runtime, Pid, Subject} = Runtime,
    case erlang:is_process_alive(Pid) of
        false ->
            {error, runtime_stopped};

        true ->
            Id = erlang:make_ref(),
            gleam@erlang@process:send(Subject, {register, Id}),
            {ok, {subscription_key, Id}}
    end.

-file("src/rx/runtime.gleam", 139).
-spec set_teardown(runtime(), subscription_key(), fun(() -> nil)) -> nil.
set_teardown(Runtime, Key, Teardown) ->
    {runtime, _, Subject} = Runtime,
    {subscription_key, Id} = Key,
    gleam@erlang@process:send(Subject, {set_teardown, Id, Teardown}).

-file("src/rx/runtime.gleam", 149).
-spec dispatch(
    runtime(),
    subscription_key(),
    rx@protocol:kind(),
    fun(() -> nil)
) -> nil.
dispatch(Runtime, Key, Kind, Work) ->
    {runtime, _, Subject} = Runtime,
    {subscription_key, Id} = Key,
    gleam@erlang@process:send(Subject, {notify, Id, Kind, Work}).

-file("src/rx/runtime.gleam", 160).
-spec cancel(runtime(), subscription_key()) -> nil.
cancel(Runtime, Key) ->
    {runtime, _, Subject} = Runtime,
    {subscription_key, Id} = Key,
    gleam@erlang@process:send(Subject, {cancel, Id}).

-file("src/rx/runtime.gleam", 175).
?DOC(
    " Register serialized state for an async flattening operator.\n"
    "\n"
    " This does not create another actor. Queue state is stored inside the existing\n"
    " Runtime actor. `on_drain` runs after the input has completed and all accepted\n"
    " work has completed and emitted.\n"
    "\n"
    " Like subscription registration, flow registration is one-way and reentrant:\n"
    " callbacks running on the runtime actor may construct and subscribe nested\n"
    " flows without waiting for the actor to reply to itself.\n"
).
-spec register_flow(runtime(), integer(), flow_order(), fun(() -> nil)) -> {ok,
        flow_key()} |
    {error, runtime_error()}.
register_flow(Runtime, Concurrency, Order, On_drain) ->
    case Concurrency > 0 of
        false ->
            {error, invalid_concurrency};

        true ->
            {runtime, Pid, Subject} = Runtime,
            case erlang:is_process_alive(Pid) of
                false ->
                    {error, runtime_stopped};

                true ->
                    Id = erlang:make_ref(),
                    gleam@erlang@process:send(
                        Subject,
                        {register_flow, Id, Concurrency, Order, On_drain}
                    ),
                    {ok, {flow_key, Id}}
            end
    end.

-file("src/rx/runtime.gleam", 202).
?DOC(
    " Add one unit of work to a flow in FIFO input order.\n"
    "\n"
    " The runtime assigns a monotonically increasing sequence number. `start` is\n"
    " invoked only when a concurrency slot is available and returns the physical\n"
    " cancellation callback for that unit of work.\n"
).
-spec enqueue_flow(runtime(), flow_key(), fun((integer()) -> fun(() -> nil))) -> nil.
enqueue_flow(Runtime, Key, Start) ->
    {runtime, _, Subject} = Runtime,
    {flow_key, Id} = Key,
    gleam@erlang@process:send(Subject, {enqueue_flow, Id, Start}).

-file("src/rx/runtime.gleam", 217).
?DOC(
    " Report completion of an asynchronously running flow item.\n"
    "\n"
    " `deliver` is a zero-argument typed closure capturing the successful value or\n"
    " error. This lets the runtime buffer/reorder work without erasing values to\n"
    " `Dynamic`.\n"
).
-spec complete_flow(
    runtime(),
    flow_key(),
    integer(),
    flow_completion(),
    fun(() -> nil)
) -> nil.
complete_flow(Runtime, Key, Sequence, Completion, Deliver) ->
    {runtime, _, Subject} = Runtime,
    {flow_key, Id} = Key,
    gleam@erlang@process:send(
        Subject,
        {complete_flow, Id, Sequence, Completion, Deliver}
    ).

-file("src/rx/runtime.gleam", 229).
-spec finish_flow_input(runtime(), flow_key()) -> nil.
finish_flow_input(Runtime, Key) ->
    {runtime, _, Subject} = Runtime,
    {flow_key, Id} = Key,
    gleam@erlang@process:send(Subject, {finish_flow_input, Id}).

-file("src/rx/runtime.gleam", 236).
?DOC(" Fail the upstream input itself and cancel all active projected work.\n").
-spec fail_flow_input(runtime(), flow_key(), fun(() -> nil)) -> nil.
fail_flow_input(Runtime, Key, Deliver_error) ->
    {runtime, _, Subject} = Runtime,
    {flow_key, Id} = Key,
    gleam@erlang@process:send(Subject, {fail_flow_input, Id, Deliver_error}).

-file("src/rx/runtime.gleam", 246).
-spec cancel_flow(runtime(), flow_key()) -> nil.
cancel_flow(Runtime, Key) ->
    {runtime, _, Subject} = Runtime,
    {flow_key, Id} = Key,
    gleam@erlang@process:send(Subject, {cancel_flow, Id}).

-file("src/rx/runtime.gleam", 257).
?DOC(
    " Stop the runtime after cancelling every active subscription and flow.\n"
    "\n"
    " This call is intentionally asynchronous so it is safe to invoke from an\n"
    " observer callback running on the runtime actor itself. Active source\n"
    " teardowns and Future cancellation callbacks run before the actor exits.\n"
).
-spec stop(runtime()) -> nil.
stop(Runtime) ->
    {runtime, _, Subject} = Runtime,
    gleam@erlang@process:send(Subject, stop).

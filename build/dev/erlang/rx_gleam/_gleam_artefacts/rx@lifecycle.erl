-module(rx@lifecycle).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/lifecycle.gleam").
-export([initial/0, transition/2]).
-export_type([state/0, action/0, command/0]).

-type state() :: {active, rx@protocol:phase(), boolean()} | closed.

-type action() :: install_teardown | {notify, rx@protocol:kind()} | cancel.

-type command() :: deliver |
    run_stored_teardown |
    run_incoming_teardown |
    {report_protocol_error, rx@protocol:protocol_error()} |
    report_duplicate_teardown.

-file("src/rx/lifecycle.gleam", 22).
-spec initial() -> state().
initial() ->
    {active, open, false}.

-file("src/rx/lifecycle.gleam", 26).
-spec transition(state(), action()) -> {state(), list(command())}.
transition(State, Action) ->
    case {State, Action} of
        {closed, install_teardown} ->
            {closed, [run_incoming_teardown]};

        {closed, {notify, _}} ->
            {closed, []};

        {closed, cancel} ->
            {closed, []};

        {{active, open, false}, install_teardown} ->
            {{active, open, true}, []};

        {{active, terminated, false}, install_teardown} ->
            {closed, [run_incoming_teardown]};

        {{active, Phase, true}, install_teardown} ->
            {{active, Phase, true},
                [run_incoming_teardown, report_duplicate_teardown]};

        {{active, Phase@1, Teardown_ready}, {notify, Kind}} ->
            case rx@protocol:transition(Phase@1, Kind) of
                {error, Reason} ->
                    {{active, Phase@1, Teardown_ready},
                        [{report_protocol_error, Reason}]};

                {ok, open} ->
                    {{active, open, Teardown_ready}, [deliver]};

                {ok, terminated} ->
                    case Teardown_ready of
                        true ->
                            {closed, [deliver, run_stored_teardown]};

                        false ->
                            {{active, terminated, false}, [deliver]}
                    end
            end;

        {{active, _, true}, cancel} ->
            {closed, [run_stored_teardown]};

        {{active, _, false}, cancel} ->
            {closed, []}
    end.

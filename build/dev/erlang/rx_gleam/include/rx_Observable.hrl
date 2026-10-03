-record(observable, {
    subscribe_ :: fun((rx@runtime:runtime(), rx:observer(any(), any())) -> {ok,
            rx:subscription()} |
        {error, rx@runtime:runtime_error()})
}).

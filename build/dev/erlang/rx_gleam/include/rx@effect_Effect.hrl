-record(effect, {
    start :: fun((fun(({ok, any()} | {error, any()}) -> nil)) -> fun(() -> nil))
}).

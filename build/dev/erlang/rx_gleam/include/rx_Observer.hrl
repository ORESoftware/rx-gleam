-record(observer, {
    on_next :: fun((any()) -> nil),
    on_error :: fun((any()) -> nil),
    on_complete :: fun(() -> nil)
}).

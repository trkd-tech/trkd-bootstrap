def start_kite_ticker(tokens, on_tick):
    ...
    def on_ticks(ws, ticks):
        for t in ticks:
            on_tick(t)

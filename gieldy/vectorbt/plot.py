import vectorbt as vbt

vbt.settings['plotting']['layout']['width'] = 1300
vbt.settings['plotting']['layout']['height'] = 650
vbt.settings.set_theme("seaborn")


def plot_base(portfolio, price_df, entries, exits):
    pf = portfolio

    fig = pf.plot(
        subplots=[("price", dict(title="Price", yaxis_kwargs=dict(title="Price"))), "trades", "trade_pnl", "cash",
                  "cum_returns", "drawdowns", ])

    fig = price_df.vbt.ohlc.plot(plot_type="candlestick", show_volume=False, ohlc_add_trace_kwargs=dict(row=1, col=1),
                                 fig=fig, xaxis=dict(rangeslider_visible=False))
    fig = entries.vbt.signals.plot_as_entry_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1), fig=fig)
    fig = exits.vbt.signals.plot_as_exit_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1), fig=fig)

    return fig

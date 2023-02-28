def plot_base(portfolio, price_df):
    pf = portfolio
    fig = pf.plot(subplots=[("price", dict(title="Price", group_id_labels=True, yaxis_kwargs=dict(title="Price"))
                             ), "value", "trades", "cum_returns", "drawdowns", "cash"])
    fig = price_df.vbt.ohlc.plot(plot_type="candlestick", show_volume=False,
                                 ohlc_add_trace_kwargs=dict(row=1, col=1), xaxis=dict(rangeslider_visible=False),
                                 fig=fig)
    fig = pf.orders.plot(add_trace_kwargs=dict(row=1, col=1), buy_trace_kwargs=dict(marker=dict(color="blue")),
                         sell_trace_kwargs=dict(marker=dict(color="black")),
                         close_trace_kwargs=dict(opacity=0, line=dict(color="black")), fig=fig)
    # fig = entries.vbt.signals.plot_as_entry_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1),
    #                                                 trace_kwargs=dict(marker=dict(color="deepskyblue")), fig=fig)
    # fig = exits.vbt.signals.plot_as_exit_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1),
    #                                              trace_kwargs=dict(marker=dict(color="orange")), fig=fig)

    return fig

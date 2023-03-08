import numpy as np
import vectorbt as vbt


class KeltnerStrat:
    def keltner_strat(self, price_df):
        keltner = vbt.IndicatorFactory.from_pandas_ta("kc").run(high=price_df["high"], low=price_df["low"],
                                                                close=price_df["close"], length=self.PERIOD,
                                                                scalar=self.DEVIATION)
        upper_band = keltner.kcue.to_numpy()
        lower_band = keltner.kcle.to_numpy()
        trend = np.where(price_df.close < lower_band, 1, 0)
        trend = np.where(price_df.close > upper_band, -1, trend)

        entries = trend == 1
        exits = trend == -1

        return entries, exits, keltner

    def keltner_print(self, keltner, fig):
        fig = keltner.kcue.vbt.plot(
            trace_kwargs=dict(name="Upper Band", opacity=0.55, line=dict(color="darkslateblue")),
            add_trace_kwargs=dict(row=1, col=1), fig=fig)
        fig = keltner.kcle.vbt.plot(
            trace_kwargs=dict(name="Lower Band", opacity=0.55, line=dict(color="darkslateblue")),
            add_trace_kwargs=dict(row=1, col=1), fig=fig)

        return fig

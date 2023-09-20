from functools import partial

import numpy as np
import pandas as pd
import vectorbtpro as vbt
from cyberoasisprojectreborn.utils.logger_custom import default_logger as logger

from cyberoasisprojectreborn.utils.utility import resample_datetime_index


class MomentumStrat:
    """Class with momentum strat"""

    def momentum_strat(self, vbt_data: vbt.Data, periods: dict):
        """Momentum strat passing variables to allocation functions"""
        rebalance_indexes = {
            f"{rebalance_tf}_rl_tf": resample_datetime_index(dt_index=vbt_data.index, resample_tf=rebalance_tf) for
            rebalance_tf in periods["REBALANCE"]}
        allocation_function = partial(self._allocation_function, vbt_data=vbt_data)
        pf_opt = vbt.PFO.from_allocate_func(
            vbt_data.symbol_wrapper,
            allocation_function,
            vbt.RepEval("wrapper.columns"),
            vbt.Rep("i"),
            vbt.Param(periods["MOMENTUM"]),
            vbt.Param(periods["NATR"]),
            vbt.Param(periods["BTC_SMA"]),
            vbt.Param(periods["TOP_NUMBER"]),
            on=vbt.Param(rebalance_indexes),
        )

        return pf_opt.simulate(vbt_data, bm_close=vbt_data.get(symbols="BTC/USDT", columns="Close"))

    def _allocation_function(self, columns: pd.DataFrame.columns, i: int, momentum_period: int, NATR_period: int,
                             btc_sma_p: int, top_number: int, vbt_data: vbt.Data):
        """Required by VBT allocation logic"""
        if i == 0:
            self.allocations = MomentumAllocation().allocation_momentum_ranking(vbt_data=vbt_data,
                                                                                momentum_period=momentum_period,
                                                                                NATR_period=NATR_period,
                                                                                btc_sma_p=btc_sma_p,
                                                                                top_number=top_number)

        return self.allocations[columns].iloc[i]


class MomentumAllocation:
    """Class with momentum allocation logic"""

    def allocation_momentum_ranking(self,
                                    vbt_data: vbt.Data,
                                    momentum_period: int,
                                    NATR_period: int = None,
                                    btc_sma_p: int = None,
                                    only_positive: bool = False,
                                    backtest_trim: bool = False,
                                    top_number: int = None,
                                    top_decimal: float = None
                                    ) -> pd.DataFrame:
        """Create allocation dataframe that depend on momentum ranking and inverse volatility"""
        logger.info("Calculating momentum ranking for pairs histories")
        if not (top_decimal or top_number):
            raise ValueError("Please provide either top decimal or top number")
        if not top_number:
            top_number = int(len(vbt_data.data) * top_decimal)

        top_momentum_pairs = self._momentum_rank(vbt_data=vbt_data, momentum_period=momentum_period,
                                                 only_positive=only_positive, backtest_trim=backtest_trim,
                                                 top_number=top_number)

        if NATR_period:
            natr_data = vbt_data.run("talib_NATR", timeperiod=NATR_period).real
            natr_data.columns = natr_data.columns.droplevel(0)
            inv_vol_data = 1 / natr_data

            top_pairs_inv_vol = inv_vol_data.where(top_momentum_pairs, other=np.nan)
            sum_top_pairs_inv_vol = top_pairs_inv_vol.sum(axis=1)
            allocations = top_pairs_inv_vol.div(sum_top_pairs_inv_vol, axis=0)
        else:
            sum_top_pairs = top_momentum_pairs.sum(axis=1)
            allocations = top_momentum_pairs.div(sum_top_pairs, axis=0)

        if btc_sma_p:
            btc_close_data = vbt_data.get(symbols="BTC/USDT", columns="Close")
            btc_data = vbt.indicators.MA.run(close=btc_close_data, window=btc_sma_p).ma_below(btc_close_data)
            allocations = allocations.where(btc_data, other=np.nan)

        allocations = allocations.replace(0, np.nan)

        return allocations

    def _momentum_rank(self, vbt_data: vbt.Data, momentum_period: int, only_positive: bool, backtest_trim: bool,
                       top_number: int):
        momentum_data = self._momentum_calc_for_vbt_data(vbt_data=vbt_data, momentum_period=momentum_period)
        momentum_data.columns = momentum_data.columns.droplevel(0)

        if only_positive:
            momentum_data = momentum_data.where(momentum_data > 0, other=np.nan)

        if backtest_trim:
            avg_momentums = momentum_data.mean()
            lower_cutoff = avg_momentums.quantile(0.05)
            upper_cutoff = avg_momentums.quantile(0.95)
            momentum_data.loc[:, (avg_momentums < lower_cutoff) | (avg_momentums > upper_cutoff)] = 0

        ranked_df = momentum_data.rank(axis=1, method="max", ascending=False)
        top_momentum_pairs = ranked_df <= top_number

        return top_momentum_pairs

    @staticmethod
    def _momentum_calc_for_vbt_data(vbt_data: vbt.Data, momentum_period: int) -> dict[str: pd.DataFrame]:
        """Calculate momentum for vbt data"""
        returns = np.log(vbt_data.get("Close"))
        x = np.arange(len(returns))
        slope = vbt.OLS.run(x=x, y=returns, window=momentum_period).slope * 100
        correlation_func = vbt.IF.from_expr("@out_corr:rolling_corr_nb(@in_x, @in_y, @p_window)")
        rvalue = correlation_func.run(x, returns, momentum_period).corr

        return round(slope * (rvalue ** 2), 4)

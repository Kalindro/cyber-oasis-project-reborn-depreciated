import numpy as np
import pandas as pd
import vectorbtpro as vbt
from loguru import logger


class MomentumAllocation:
    def allocation_momentum_ranking(self, vbt_data: vbt.Data, momentum_period: int, NATR_period: int,
                                    top_decimal: float = None, top_number: int = None) -> pd.DataFrame:
        """Create allocation dataframe that depend on momentum ranking and inverse volatility"""
        logger.info("Calculating momentum ranking for pairs histories")
        if not (top_decimal or top_number):
            raise ValueError("Please provide either top decimal or top number")
        if not top_number:
            top_number = int(len(vbt_data.data) * top_decimal)

        btc_close_data = vbt_data.get(symbols="BTC/USDT", columns="Close")
        btc_sma = vbt.indicators.MA.run(close=btc_close_data).ma

        momentum_data = self._momentum_calc_for_vbt_data(vbt_data=vbt_data, momentum_period=momentum_period)
        momentum_data.columns = momentum_data.columns.droplevel(0)

        natr_data = vbt_data.run("talib_NATR", timeperiod=NATR_period).real
        natr_data.columns = natr_data.columns.droplevel(0)

        allocations = self._allocation_calc_for_vbt_data(momentum_data=momentum_data, natr_data=natr_data,
                                                         top_number=top_number)

        return allocations

    @staticmethod
    def _allocation_calc_for_vbt_data(momentum_data: pd.DataFrame, natr_data: pd.DataFrame, top_number: int = None):
        """Calculate allocation for vbt data"""
        only_positive = False

        if only_positive:
            momentum_data = momentum_data.where(momentum_data > 0)

        ranked_df = momentum_data.rank(axis=1, method="max", ascending=False)
        inv_vol_data = 1 / natr_data

        top_pairs = ranked_df <= top_number
        top_pairs_inv_vol = inv_vol_data.where(top_pairs, other=np.nan)
        sum_top_pairs_inv_vol = top_pairs_inv_vol.sum(axis=1)
        allocations = top_pairs_inv_vol.div(sum_top_pairs_inv_vol, axis=0).replace(0, np.nan)

        return allocations

    @staticmethod
    def _momentum_calc_for_vbt_data(vbt_data: vbt.Data, momentum_period: int) -> dict[str: pd.DataFrame]:
        """Calculate momentum for vbt data"""
        returns = np.log(vbt_data.get("Close"))
        x = np.arange(len(returns))
        slope = vbt.OLS.run(x=x, y=returns, window=momentum_period).slope * 100
        correlation_func = vbt.IF.from_expr("@out_corr:rolling_corr_nb(@in_x, @in_y, @p_window)")
        rvalue = correlation_func.run(x, returns, momentum_period).corr

        return slope * (rvalue ** 2)

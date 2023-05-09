import pandas as pd
from pandas import DataFrame as df

from exchange.get_history import GetFullHistoryDF
from exchange.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru
from utils.utils import excel_save_formatted_naive

logger = ConfigureLoguru().info_level()


class _BaseSettings(FundamentalSettings):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.TIMEFRAME = "1h"
        self.VOL_QUANTILE_DROP = 0.25
        self.DAYS_WINDOWS = [1, 2, 3, 7, 14, 31]
        self.NUMBER_OF_LAST_CANDLES = self.number_of_last_candles

    @property
    def number_of_last_candles(self):
        return (max(self.DAYS_WINDOWS) + 3) * 24

    @property
    def _min_data_length(self):
        return max(self.DAYS_WINDOWS) * 24


class PerformanceRankAnalysis(_BaseSettings):
    """Main analysis class"""

    def main(self) -> None:
        """Main function runing the analysis"""
        vbt_data = GetFullHistoryDF(pairs_list=self.pairs_list, timeframe=self.TIMEFRAME,
                                    number_of_last_candles=self.NUMBER_OF_LAST_CANDLES, API=self.API,
                                    min_data_length=self._min_data_length,
                                    vol_quantile_drop=self.VOL_QUANTILE_DROP).get_full_history()
        hist_dict = {f"hist_{days}d": vbt_data.iloc[days * (-24):] for days in self.DAYS_WINDOWS}

        hist_24h = hist_dict["hist_1d"]
        hist_prev_24h = vbt_data.iloc[-48:-24]
        hist_3d = hist_dict["hist_3d"]
        hist_7d = hist_dict["hist_7d"]

        hist_24h_median_vol = hist_24h.get(columns="Volume").median()
        hist_3d_median_vol = hist_3d.get(columns="Volume").median()
        hist_7d_median_vol = hist_7d.get(columns="Volume").median()

        price_24h_change = self._calculate_price_change(hist_24h.get(columns="Close"))
        price_prev_24h_change = self._calculate_price_change(hist_prev_24h.get(columns="Close"))
        vol_3d_incr = round((hist_3d_median_vol / hist_24h_median_vol), 2).to_dict()
        vol_7d_incr = round((hist_7d_median_vol / hist_24h_median_vol), 2).to_dict()

        full_performance_df = df.from_dict(
            {pair: {"24h_change": price_24h_change[pair], "prev_24h_change": price_prev_24h_change[pair],
                    "3d_vol_incr": vol_3d_incr[pair], "7d_vol_incr": vol_7d_incr[pair]
                    } for pair, _ in vol_3d_incr.items()}, orient="index")

        excel_save_formatted_naive(full_performance_df, filename="performance.xlsx", global_cols_size=15)
        logger.success("Saved excel")

        # Printing
        try:
            print(full_performance_df)
            print("24H:")
            btc_perf = f'{round(full_performance_df.loc["BTC/USDT", "24h_change"] * 100, 2):.2f}'
            market_perf = f'{round(full_performance_df["24h_change"].mean() * 100, 2):.2f}'
            positive_percent = f'{round(full_performance_df["24h_change"] > 0).sum() / len(full_performance_df) * 100:.2f}'
            negative_percent = f'{round(full_performance_df["24h_change"] < 0).sum() / len(full_performance_df) * 100:.2f}'
            print(f"BTC performance: {btc_perf}%")
            print(f"Market performance: {market_perf}%")
            print(f"Positive coins: {positive_percent}% | Negative coins: {negative_percent}%")
            print()
            print("Previous 24H:")
            btc_prev_perf = f'{round(full_performance_df.loc["BTC/USDT", "prev_24h_change"] * 100, 2):.2f}'
            market_prev_perf = f'{round(full_performance_df["prev_24h_change"].mean() * 100, 2):.2f}'
            positive_prev_percent = f'{round(full_performance_df["prev_24h_change"] > 0).sum() / len(full_performance_df) * 100:.2f}'
            negative_prev_percent = f'{round(full_performance_df["prev_24h_change"] < 0).sum() / len(full_performance_df) * 100:.2f}'
            print(f"BTC performance: {btc_prev_perf}%")
            print(f"Market performance: {market_prev_perf}%")
            print(f"Positive coins: {positive_prev_percent}% | Negative coins: {negative_prev_percent}%")

        except Exception as err:
            print(err)
            pass

    @staticmethod
    def _calculate_price_change(hist_df: pd.DataFrame) -> float:
        """Function counting price change in %"""
        performance = (hist_df.iloc[-1] - hist_df.iloc[0]) / hist_df.iloc[0]

        return performance


if __name__ == "__main__":
    PerformanceRankAnalysis().main()

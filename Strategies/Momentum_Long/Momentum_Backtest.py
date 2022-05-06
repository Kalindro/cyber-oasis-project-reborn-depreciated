import backtrader as bt
from datetime import date, datetime
import numpy as np
import time
import itertools
from scipy.stats import linregress
import pandas as pd
from pandas import DataFrame as df
import collections
from beepy import beep
import functools
import multiprocessing

from Gieldy.Refractor_general.Get_filtered_tickers import get_filtered_tickers
from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Backtrader.Analyzers import AddAnalyzers


class RunBacktest:
    def __init__(self):
        self.backtest_settings = dict(
            OPTIMIZE=False,
            SAVE_EXCEL=True,
            FRESH_LIVE_TICKERS=False,
            FORBID_EXCHANGE_TOKENS=False,
            TICKERS_MODE=2,  # 1 = One coin 2 = Few coins 3 = Whole exchange
            WIGGLED_ROOM=0.95,
            TIMEFRAME="4H",
            THE_ONE_COIN=["GTO/USDT"],
            BLACK_LIST_ENABLE=False,
            BLACK_LIST=["SNX/USDT", "UTK/USDT", "COS/USDT", "CTXC/USDT", "AION/USDT", "BEL/USDT", "MTL/USDT", "EOS/USDT", "WTC/USDT",
                        "LTC/USDT", "KMD/USDT", "LRC/USDT", "ARDR/USDT", "AKRO/USDT", "BCH/USDT", "BAT/USDT", "WAN/USDT", "BTS/USDT",
                        "KEY/USDT", "GTO/USDT", "ONT/USDT"],
            BASIC_ORDER_LOG=False,
            DETAILED_ORDER_LOG=False,
            TRADE_LOG=False,
            START=date.fromisoformat("2020-01-01"),
            END=date.fromisoformat("2022-01-01"),
        )

        self.indicators_params = dict(
            MOMENTUM_PERIOD=tuple(range(100, 200, 50)),    # 100
            ETH_SMA_PERIOD=tuple((160, )),     # 160
            ATRR_PERIOD=tuple((42, )),         # 42
            MOMENTUM_FILTER=tuple((0, )),      # 0
            RVALUE_FILTER=tuple((45, )),       # 45
            NUM_ELITE_COINS=tuple((25, )),     # 25
            REBALANCE_PERIOD=tuple((2, )),     # 2
        )

        self.MOMENTUM_PERIOD = self.indicators_params["MOMENTUM_PERIOD"]
        self.ETH_SMA_PERIOD = self.indicators_params["ETH_SMA_PERIOD"]
        self.ATRR_PERIOD = self.indicators_params["ATRR_PERIOD"]
        self.MOMENTUM_FILTER = self.indicators_params["MOMENTUM_FILTER"]
        self.RVALUE_FILTER = self.indicators_params["RVALUE_FILTER"]
        self.NUM_ELITE_COINS = self.indicators_params["NUM_ELITE_COINS"]
        self.REBALANCE_PERIOD = self.indicators_params["REBALANCE_PERIOD"]

    class PandasData(bt.feeds.PandasData):
        params = (
            ("datetime", None),
            ("open", "open"),
            ("high", "high"),
            ("low", "low"),
            ("close", "close"),
            ("volume", "volume"),
            ("openinterest", None),
        )

    # def scenarios_prep(self):
    #     """ Returns: List of dict combinations of indicators values for parameter optimization"""
    #     test_params = self.indicators_params.copy()
    #
    #     keys = test_params.keys()
    #     values = test_params.values()
    #
    #     scenarios = [dict(zip(keys, combination)) for combination in itertools.product(*values)]
    #
    #     print(f"There will be {len(scenarios)} backtests run")
    #
    #     return scenarios

    def scene_prep(self):
        """ Returns: List of dict combinations of indicators values for parameter optimization"""

        test_params = self.indicators_params.copy()

        keys = test_params.keys()
        values = test_params.values()
        scene = test_params

        scenarios = [dict(zip(keys, combination)) for combination in itertools.product(*values)]

        print(f"There will be {len(scenarios)} backtests run")

        return scene

    def tickers_prep(self, API, base):
        """Returns: List of tickers according to ticker mode"""

        tickers = []
        if self.backtest_settings["TICKERS_MODE"] == 1:
            tickers = self.backtest_settings["THE_ONE_COIN"]

        elif self.backtest_settings["TICKERS_MODE"] == 2:
            tickers = ["ETH/USDT", "LTC/USDT", "NEO/USDT", "EOS/USDT", "XRP/USDT", "BAT/USDT", "TRX/USDT", "ADA/USDT", "BNB/USDT", "BTC/USDT",
                       "LINK/USDT", "VET/USDT", "WING/USDT"]

        elif self.backtest_settings["TICKERS_MODE"] == 3:
            tickers = get_filtered_tickers(base=base, API=API, fresh_live_tickers=self.backtest_settings["FRESH_LIVE_TICKERS"],
                                           forbid_exchange_tokens=self.backtest_settings["FORBID_EXCHANGE_TOKENS"])

        return tickers

    def get_history(self, API, base):
        """Returns: List of all tickers history"""
        tickers = self.tickers_prep(API, base)

        partial_history = functools.partial(get_history_full, start=self.backtest_settings["START"], end=self.backtest_settings["END"], fresh_live_history=False,
                                            timeframe=self.backtest_settings["TIMEFRAME"], API=API)
        shark_pool = multiprocessing.Pool(processes=6)
        all_coins_history = shark_pool.imap(partial_history, tickers)
        shark_pool.close()
        shark_pool.join()

        ETH_full_history = get_history_full(pair="ETH/USDT", start=self.backtest_settings["START"], end=self.backtest_settings["END"], fresh_live_history_no_save_read=False, timeframe=self.backtest_settings["TIMEFRAME"], API=API)

        print("Got history for all the coins")

        return all_coins_history, ETH_full_history

    def cerbero_run(self, scene, all_coins_history, ETH_full_history):
        """Returns: Results of all backtest run"""
        # Prepare cerebro and add datas
        plot = True if self.backtest_settings["TICKERS_MODE"] == 1 else False

        ETH_data_ready = self.PandasData(dataname=ETH_full_history, name="ETH/USDT", plot=not plot)

        cerebro = bt.Cerebro(live=False, exactbars=False, stdstats=False, runonce=True, preload=True, optreturn=True, optdatas=True,
                             quicknotify=True, maxcpus=10)

        cerebro.adddata(ETH_data_ready)

        for coin_history in all_coins_history:
            if len(coin_history) > max(self.MOMENTUM_PERIOD[-1], self.ETH_SMA_PERIOD[-1], self.ATRR_PERIOD[-1]):
                cerebro.adddata(self.PandasData(dataname=coin_history, plot=plot, name=str(coin_history["pair"].iloc[0])))

        print("Added all data")

        # Add analyzers and observers
        cerebro = AddAnalyzers(cerebro).add_analyzers(OPTIMIZE=self.backtest_settings["OPTIMIZE"], TICKERS_MODE=self.backtest_settings["TICKERS_MODE"])
        cerebro.addanalyzer(bt.analyzers.TimeReturn, data=ETH_data_ready, timeframe=bt.TimeFrame.NoTimeFrame, _name="benchmark_TR")
        cerebro.addanalyzer(bt.analyzers.TimeReturn, timeframe=bt.TimeFrame.NoTimeFrame, _name="strat_TR")

        cerebro.addstrategy(WholeStrategySetup.MainMomentumStrategy, **scene)

        print("Running backtest")
        results = cerebro.run()

        # Plot
        if not self.backtest_settings["OPTIMIZE"]:
            cerebro.plot(style="candlestick", volume=False, barup="limegreen", bardown="tomato")

        return results

    def run_backtest(self, API, base):
        """Function running backtest"""
        scene = self.scene_prep()

        start_test = time.time()

        all_coins_history, ETH_full_history = self.get_history(API, base)
        all_results = self.cerbero_run(scene=scene, all_coins_history=all_coins_history, ETH_full_history=ETH_full_history)

        print(list(all_results))

        print(f"Elapsed: {(time.time() - start_test):.2f}")

        return all_results

    def results_controller(self, API, base):
        """Receives all results from backtests and outputs into neat excel file"""

        print("Starting backtest")
        results = self.run_backtest(API, base)

        name = API["name"]
        backtest_settings = self.backtest_settings

        all_runs_dataframe = df()
        for i, strat in enumerate(results):
            run_data = {
                "Run number": [i],
                "End value": round(strat.analyzers.portfolio_analyzer.get_analysis(), 2),
                "End perc PnL": round(strat.total_percent_pnl, 2) if not backtest_settings["OPTIMIZE"] else 0,
                "Max DD": round(strat.analyzers.drawdown.get_analysis()["max"]["drawdown"], 2),
                "Value": round(
                    strat.analyzers.portfolio_analyzer.get_analysis() / (strat.analyzers.drawdown.get_analysis()["max"]["drawdown"] / 100),
                    2) if
                strat.analyzers.drawdown.get_analysis()["max"]["drawdown"] > 0 else 0,
                "Perc value": round(strat.total_percent_pnl / (strat.analyzers.drawdown.get_analysis()["max"]["drawdown"] / 100), 2) if
                strat.analyzers.drawdown.get_analysis()["max"]["drawdown"] > 0 and not backtest_settings["OPTIMIZE"] else 0,
                "SQN": round(strat.analyzers.sqn.get_analysis()["sqn"], 4),
                "Strat TR": round(list(strat.analyzers.strat_TR.get_analysis().values())[0], 2),
                "Bench TR": round(list(strat.analyzers.benchmark_TR.get_analysis().values())[0], 2),
                "Trades": strat.analyzers.sqn.get_analysis()["trades"],
            }
            run_parameters = vars(strat.params)
            run_data.update(run_parameters)
            print("")
            [print(f"{k}: {v}") for k, v in run_data.items()]
            ziobro_ty_kurwo = pd.DataFrame.from_dict(run_data, dtype=float)
            all_runs_dataframe = all_runs_dataframe.append(ziobro_ty_kurwo, ignore_index=True)

        if backtest_settings["SAVE_EXCEL"]:
            # Excel save
            basic_file_name = f"{name}"

            if backtest_settings["OPTIMIZE"]:
                basic_file_name = f"OPTI {basic_file_name}, {self.MOMENTUM_PERIOD} to {self.MOMENTUM_PERIOD} Mom, {self.ETH_SMA_PERIOD} to {self.ETH_SMA_PERIOD} ETH SMA"
            else:
                basic_file_name = f"{basic_file_name}, {self.MOMENTUM_PERIOD} Mom, {self.ETH_SMA_PERIOD} ETH SMA"

            addition_name = f"St {backtest_settings['START']} En {backtest_settings['END']}, {datetime.now().strftime('%Y-%m-%d %H@%M')}"

            writer = pd.ExcelWriter(f"{basic_file_name}, {addition_name}.xlsx", engine="xlsxwriter")
            workbook = writer.book

            all_runs_dataframe.to_excel(writer)

            # Excel formatting
            results_sheet = writer.sheets["Sheet1"]
            currency_format = workbook.add_format({"num_format": "$#,##0.00"})
            percent_format = workbook.add_format({"num_format": "0.00##\%"})
            number_format = workbook.add_format({"num_format": "#,##0.00"})
            results_sheet.set_column("B:P", 13)
            results_sheet.set_column("C:C", 13, currency_format)
            results_sheet.set_column("D:E", 13, percent_format)
            results_sheet.set_column("F:F", 13, number_format)
            results_sheet.set_column("H:H", 9)
            results_sheet.set_column("K:K", 9)
            results_sheet.set_column("L:P", 18)
            writer.save()

        print("BEEPING")
        beep(sound=1)


class WholeStrategySetup:
    # Main momentum indicator
    class Momentum(bt.ind.PeriodN):
        lines = ("momentum", "rvalue")
        params = dict(period=0, rvalue_filter=0)
        plotinfo = dict(plot=False)

        def next(self):
            returns_fast = np.log(self.data.get(size=int(self.p.period * 0.5)))
            returns_slow = np.log(self.data.get(size=int(self.p.period)))
            x_fast = np.arange(len(returns_fast))
            x_slow = np.arange(len(returns_slow))
            slope_fast, _, rvalue_fast, _, _ = linregress(x_fast, returns_fast)
            slope_slow, _, rvalue_slow, _, _ = linregress(x_slow, returns_slow)
            momentum_fast = slope_fast * (rvalue_fast ** 2) * 10000
            momentum_slow = slope_slow * (rvalue_slow ** 2) * 10000
            self.lines.momentum[0] = round((momentum_fast + momentum_slow) / 2, 2)
            self.lines.rvalue[0] = round(((rvalue_fast ** 2) + (rvalue_slow ** 2)) / 2, 4) if (rvalue_fast and rvalue_slow) > (
                    self.p.rvalue_filter / 100) else 0

    class MainMomentumStrategy(bt.Strategy):
        params = RunBacktest().indicators_params

        def log(self, txt):
            dt = self.datas[0].datetime.datetime(0)
            print("%s, %s" % (dt.isoformat(), txt))

        def __init__(self):
            self.backtest_settings = RunBacktest().backtest_settings
            self.total_percent_pnl = 0
            self.ETH = self.datas[0]
            self.coins_indicators = collections.defaultdict(dict)

            self.ETH_SMA = bt.indicators.SimpleMovingAverage(self.ETH.close, period=self.p.ETH_SMA_PERIOD)

            for d in self.datas[1:]:
                ATR = bt.indicators.ATR(d, period=self.p.ATRR_PERIOD)
                curr_momentum = WholeStrategySetup.Momentum(d, period=self.p.MOMENTUM_PERIOD, rvalue_filter=self.p.RVALUE_FILTER)
                self.coins_indicators[d]["ATR"] = ATR
                self.coins_indicators[d]["ATRR"] = 1 / (ATR / d.close)
                self.coins_indicators[d]["momentum"] = curr_momentum.lines.momentum
                self.coins_indicators[d]["rvalue"] = curr_momentum.lines.rvalue
                self.coins_indicators[d]["SMA"] = bt.indicators.SimpleMovingAverage(d, period=int(self.p.ETH_SMA_PERIOD / 2))
                self.coins_indicators[d]["perc_pnl"] = 0
                self.coins_indicators[d]["trades_number"] = 0

        def notify_trade(self, trade):
            if trade.isclosed:
                self.total_percent_pnl += (trade.pnlcomm / self.broker.get_value() * 100)
                self.coins_indicators[trade.data]["perc_pnl"] += (trade.pnlcomm / self.broker.get_value() * 100)
                self.coins_indicators[trade.data]["trades_number"] += 1

            if self.backtest_settings["TRADE_LOG"]:
                self.log(
                    f"PNL: ${trade.pnlcomm:.2f}\t"
                    f"Coin: {trade.data._name}\t"
                )

        def notify_order(self, order):
            base_logging = (f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                            f"Status: {order.getstatusname()}\t"
                            f"Order size: {order.size:,.4f}\t"
                            f"Create Price: {order.created.price:,.4f}\t"
                            f"Order cost: {order.size * order.created.price:,.2f}\t"
                            f"Cash: {self.broker.get_cash():,.2f}\t"
                            f"Acc Value: {self.broker.get_value():,.2f}\t"
                            f"Position: {self.getposition(order.data).size:,.4f}\t"
                            f"Coin: {order.data._name}\t"
                            )

            if order.status in [order.Margin, order.Rejected]:
                self.log(
                    f"Order {order.ref} Margin/Rejected\t" + base_logging
                )

            if self.backtest_settings["BASIC_ORDER_LOG"] and (order.status in [order.Completed]):
                self.log(
                    f"EXECUTED {order.ref}\t" + base_logging
                )

            if self.backtest_settings["DETAILED_ORDER_LOG"]:
                self.log(
                    f"Order {order.ref}\t" + base_logging
                )

        def prenext(self):
            self.coins = [d for d in self.datas[1:] if len(d)]
            self.next()

        def nextstart(self):
            self.coins = self.datas[1:]
            self.next()

        def next(self):
            length = len(self)

            if length % self.p.REBALANCE_PERIOD == 0:
                self.rebalance_portfolio()

            if length % (self.p.REBALANCE_PERIOD * 2) == 0:
                self.rebalance_positions()

        # Main rebalancing section
        def rebalance_portfolio(self):
            self.all_coins_loop_list = list(
                filter(lambda x: len(x) > max(self.p.MOMENTUM_PERIOD, self.p.ETH_SMA_PERIOD, self.p.ATRR_PERIOD), self.coins))
            self.coins_ranking = df(columns=["data", "pair", "momentum", "rvalue", "ATRR", "perc_pnl"])
            for d in self.all_coins_loop_list:
                pair = d._name
                pair_atrr = self.coins_indicators[d]["ATRR"][0]
                pair_momentum = self.coins_indicators[d]["momentum"][0]
                pair_rvalue = self.coins_indicators[d]["rvalue"][0]
                pair_perc_pnl = self.coins_indicators[d]["perc_pnl"]
                if (pair_momentum < self.p.MOMENTUM_FILTER) or (pair_rvalue < (self.p.RVALUE_FILTER / 100)) or (
                        d.close < self.coins_indicators[d]["SMA"]): continue
                if self.backtest_settings["BLACK_LIST_ENABLE"]:
                    if pair in self.backtest_settings["BLACK_LIST"]: continue
                ranking_dict = {"data": [d], "pair": [pair], "momentum": [pair_momentum], "rvalue": [pair_rvalue], "ATRR": [pair_atrr],
                                "perc_pnl": [pair_perc_pnl]}
                ziobroooo_ty = pd.DataFrame.from_dict(ranking_dict)
                self.coins_ranking = self.coins_ranking.append(ziobroooo_ty, ignore_index=True)
            self.coins_ranking.sort_values(by=["momentum"], inplace=True, ascending=False)
            self.coins_ranking.reset_index(inplace=True, drop=True)
            self.elite_length = len(self.coins_ranking.head(self.p.NUM_ELITE_COINS))
            self.ATRR_elite_sum = self.coins_ranking.head(self.p.NUM_ELITE_COINS)["ATRR"].sum()

            self.backtest_pnl = df(columns=["pair", "perc_pnl", "trades_number"])
            for d in self.coins_indicators:
                pair = d._name
                pair_perc_pnl = self.coins_indicators[d]["perc_pnl"]
                pair_trades_number = self.coins_indicators[d]["trades_number"]
                ranking_dict = {"pair": [pair], "perc_pnl": [pair_perc_pnl], "trades_number": [pair_trades_number]}
                ziobroooo_ty = pd.DataFrame.from_dict(ranking_dict)
                self.backtest_pnl = self.backtest_pnl.append(ziobroooo_ty, ignore_index=True)
            self.backtest_pnl.sort_values(by=["perc_pnl"], inplace=True, ascending=False)

            # Sell coins section
            for d in self.all_coins_loop_list:
                sell_conditions = (d not in self.coins_ranking["data"].head(self.p.NUM_ELITE_COINS).values)
                if self.getposition(d) and sell_conditions:
                    self.close(d)

            if self.ETH.close < self.ETH_SMA:
                return

            # Buy coins section
            for d in self.coins_ranking["data"].head(self.p.NUM_ELITE_COINS).values:
                if not self.getposition(d):
                    playing_value = self.broker.get_value() * (self.elite_length / (self.p.NUM_ELITE_COINS + 1))
                    weight = round((self.coins_indicators[d]["ATRR"] / self.ATRR_elite_sum), 6)
                    target_size = playing_value * weight / d.close[0]
                    self.buy(d, size=target_size * self.backtest_settings["WIGGLED_ROOM"])

        def rebalance_positions(self):
            # Rebalance positions size section
            if self.ETH.close < self.ETH_SMA:
                return

            for d in self.coins_ranking["data"].head(self.p.NUM_ELITE_COINS).values:
                current_size = self.getposition(d).size
                if current_size:
                    playing_value = self.broker.get_value() * (self.elite_length / (self.p.NUM_ELITE_COINS + 1))
                    weight = round((self.coins_indicators[d]["ATRR"] / self.ATRR_elite_sum), 6)
                    target_size = playing_value * weight / d.close[0]
                    if current_size > target_size * 1.05:
                        self.sell(d, size=current_size - target_size)
                    if current_size < target_size * 0.95:
                        self.buy(d, size=(target_size - current_size) * self.backtest_settings["WIGGLED_ROOM"])

        def stop(self):
            for d in self.coins_indicators:
                pos = self.getposition(d)
                comminfo = self.broker.getcommissioninfo(d)
                pnl = comminfo.profitandloss(pos.size, pos.price, d.close[0])
                self.total_percent_pnl += (pnl / self.broker.get_value() * 100)
            self.coins_ranking[["pair", "momentum", "rvalue", "ATRR", "perc_pnl"]].to_excel("Ranking Backtest.xlsx")
            self.backtest_pnl.to_excel("Ranking Backtest PNL.xlsx")
            print(f"Compounded {self.total_percent_pnl:.2f}% profit")
            print(f"Run ${self.broker.getvalue():,.2f} complete")

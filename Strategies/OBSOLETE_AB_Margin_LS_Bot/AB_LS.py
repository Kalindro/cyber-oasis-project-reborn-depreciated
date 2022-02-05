import backtrader as bt

from datetime import date, datetime

import pandas as pd
from pandas import DataFrame as df

import collections
from beepy import beep

from functools import partial
from multiprocessing import Pool

from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Refractor_general.Get_filtered_tickers import refractor_get_backtesting_tickers


# --------------------------------------------
# Backtesting options

Fresh_tickers = False
Fresh_history = False

Always_in_the_market = True

Optimize = False
Save_excel = False

One_coin = 3  # 1 = One coin 2 = Few coins 3 = Whole exchange
The_one_coin = ["NEO/USDT"]

Order_log = False
Detailed_order_log = False
Trade_log = False

if Optimize:
    Order_log = False
    Detailed_order_log = False
    Trade_log = False

# --------------------------------------------
# Backtrader classes

class MyBuySell(bt.observers.BuySell):
    plotinfo = dict(plot = True, subplot = False, plotlinelabels = True)
    plotlines = dict(
        buy = dict(marker = "^", markersize = 10.0, color = "royalblue"),
        sell = dict(marker = "v", markersize = 10.0, color = "darkorchid")
    )

class PandasData(bt.feeds.PandasData):

    params = (
        ('datetime', None),
        ('open', 'Open'),
        ('high', 'High'),
        ('low', 'Low'),
        ('close', 'Close'),
        ('volume', 'Volume'),
        ('openinterest', None),
    )

class Portfolioanalyzer(bt.Analyzer):

    def next(self):
        self.Portfolio_ending = self.strategy.broker.get_value()

    def get_analysis(self):
        return self.Portfolio_ending

class Position_side(bt.observer.Observer):
    lines = ("Position",)

    plotinfo = dict(plot = True, subplot = True)

    def next(self):

        for data in self.datas:

            if data is not self.data:
                continue

            if self._owner.broker.get_value([data]) > 0:
                self.lines.Position[0] = 1
            elif self._owner.broker.get_value([data]) < 0:
                self.lines.Position[0] = -1
            elif self._owner.broker.get_value([data]) == 0:
                self.lines.Position[0] = 0

class MovingAverageSimpleEnvelope(bt.indicators.MovingAverageSimpleEnvelope):
    plotlines = dict(
        sma = dict(color = "red"),
        top = dict(color = "steelblue"),
        bot = dict(color = "steelblue"))

# --------------------------------------------
# Main strategy

class AB_LS_Envelope(bt.Strategy):

    # --------------------------------------------
    # Parameters

    def log(self, txt, dt = None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    params = dict(
        ATRP_period = 0,
        Envelopes_period = 0,
        ATRP_fix_global = 0,
        )

    def __init__(self):

        # --------------------------------------------
        # Indicator logic

        self.BTC = self.datas[0]
        self.Coins_indicators = collections.defaultdict(dict)

        for d in self.datas:
            self.Coins_indicators[d]["ATRP"] = bt.talib.NATR(d.high, d.low, d.close, timeperiod = self.params.ATRP_period, plotname = "ATRP") / 100 * self.params.ATRP_fix_global
            self.Coins_indicators[d]["Perc"] = self.Coins_indicators[d]["ATRP"] * 100 / 2
            self.Coins_indicators[d]["Envelope"] = MovingAverageSimpleEnvelope(d, perc = self.Coins_indicators[d]["Perc"], plotname = "SMA Envelope", period = self.params.Envelopes_period)

    # --------------------------------------------
    # Order check

    def notify_order(self, order):

        if order.status in [order.Margin, order.Rejected]:
            self.log(
                f"Order {order.ref} Margin/Rejected\t"
                f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                f"Size: {order.size:,.4f}\t"
                f"Create Price: {order.created.price:,.4f}\t"
                f"Cost: {order.size * order.created.price:,.4f}\t"
                f"Cash: {self.broker.get_cash():,.2f}\t"
                f"Value: {self.broker.get_value():,.2f}\t"
                f"Position: {self.position.size:,.4f}\t"
                f"Coin: {order.data._name}\t"
            )

        if Order_log:
            if order.status in [order.Completed]:
                    self.log(
                    f"EXECUTED {order.ref}\t"
                    f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                    f"Size: {order.size:,.4f}\t"
                    f"Create Price: {order.created.price:,.4f}\t"
                    f"Cost: {order.size * order.created.price:,.4f}\t"
                    f"Cash: {self.broker.get_cash():,.2f}\t"
                    f"Value: {self.broker.get_value():,.2f}\t"
                    f"Position: {self.position.size:,.4f}\t"
                    f"Coin: {order.data._name}\t"
                    )

            if Detailed_order_log:
                self.log(
                    f"Order: {order.ref}\t"
                    f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                    f"Status: {order.getstatusname()}\t"
                    f"Size: {order.size:,.4f}\t"
                    f"Create Price: {order.created.price:,.4f}\t"
                    f"Cost: {order.size * order.created.price:,.4f}\t"
                    f"Cash: {self.broker.get_cash():,.2f}\t"
                    f"Value: {self.broker.get_value():,.2f}\t"
                    f"Position: {self.position.size:,.4f}\t"
                    f"Coin: {order.data._name}\t"
                )

    if Trade_log:
        def notify_trade(self, trade):
            if trade.isclosed:
                self.log(
                f"PNL: ${trade.pnlcomm:.2f}\t"
                f"Coin: {trade.data._name}\t"
                )

    # --------------------------------------------
    # Execution

    def prenext(self):

        self.Coins = [d for d in self.datas[1:] if len(d)]
        self.next()

    def nextstart(self):

        self.Coins = self.datas[1:]
        self.next()

    def next(self):

        # --------------------------------------------
        # Cancel all orders

        Orders = self.broker.get_orders_open()
        if Orders:
            for Order in Orders:
                self.broker.cancel(Order)

        for i, d in enumerate(self.Coins):

            # --------------------------------------------
            # Buys

            self.Account_cash = self.broker.get_cash()
            self.Account_value = self.broker.get_value()
            Size = self.Account_value * 0.95 / len(self.Coins) / d.close

            if Always_in_the_market:

                if self.getposition(d).size > 0:
                    Close_long = self.close(d, exectype = bt.Order.Limit, price = self.Coins_indicators[d]["Envelope"].lines.top[0])
                    Open_short = self.sell(d, exectype = bt.Order.Limit, price = self.Coins_indicators[d]["Envelope"].lines.top[0], size = Size)

                if self.getposition(d).size < 0:
                    Close_short = self.close(d, exectype = bt.Order.Limit, price = self.Coins_indicators[d]["Envelope"].lines.bot[0])
                    Open_long = self.buy(d, exectype = bt.Order.Limit, price = self.Coins_indicators[d]["Envelope"].lines.bot[0], size = Size)

                if not self.getposition(d):

                    if d.low < self.Coins_indicators[d]["Envelope"].lines.bot[0]:
                        Open_long = self.buy(d, exectype = bt.Order.Stop, price = self.Coins_indicators[d]["Envelope"].lines.bot[0], size = Size)

                    if d.high > self.Coins_indicators[d]["Envelope"].lines.top[0]:
                        Open_short = self.sell(d, exectype = bt.Order.Stop, price = self.Coins_indicators[d]["Envelope"].lines.top[0], size = Size)

            if not Always_in_the_market:

                if self.getposition(d).size > 0:
                    Close_long = self.close(d, exectype = bt.Order.Limit, price = self.Coins_indicators[d]["Envelope"].lines.top[0])

                if self.getposition(d).size < 0:
                    Close_short = self.close(d, exectype = bt.Order.Limit, price = self.Coins_indicators[d]["Envelope"].lines.bot[0])

                if not self.getposition(d):

                    if d.low < self.Coins_indicators[d]["Envelope"].lines.bot[0]:
                        Open_long = self.buy(d, exectype = bt.Order.Stop, price = self.Coins_indicators[d]["Envelope"].lines.bot[0], size = Size)

                    if d.high > self.Coins_indicators[d]["Envelope"].lines.top[0]:
                        Open_short = self.sell(d, exectype = bt.Order.Stop, price = self.Coins_indicators[d]["Envelope"].lines.top[0], size = Size)

    # --------------------------------------------
    # After run execution

    def stop(self):
        print(f"Run ${self.broker.getvalue():,.2f} complete")

# --------------------------------------------
# Main loop settings

def backtest_ab(API, Base, Start = date.fromisoformat("2021-01-01") , End = date.fromisoformat("2021-08-01")):

    Name = API["Name"]

    Env_period = tuple((10, ))
    ATRP_period = tuple((60, ))
    ATRP_fix_global = tuple(range(25, 80))
    ATRP_fix_global = tuple(Number / 10 for Number in ATRP_fix_global)

    if not Optimize:
        Env_period = Env_period[0]
        ATRP_period = ATRP_period[0]
        ATRP_fix_global = ATRP_fix_global[0]

    Cerebro = bt.Cerebro(stdstats = False, runonce = True, preload = True, optreturn = True, optdatas = True, maxcpus = 10)

    # --------------------------------------------
    # Get tickers

    if One_coin == 1:
        Tickers = The_one_coin

    elif One_coin == 2:
        Tickers = ["ETH/USDT", "LTC/USDT", "NEO/USDT", "EOS/USDT", "XRP/USDT", "BAT/USDT", "TRX/USDT", "ADA/USDT", "BNB/USDT", "BTC/USDT", "LINK/USDT"]

    elif One_coin == 3:
        Tickers = refractor_get_backtesting_tickers(Base = Base, API = API, Fresh_tickers = Fresh_tickers)

    # --------------------------------------------
    # Add the data to Cerebro

    Delta = End - Start
    Days_of_history = Delta.days

    Partial_history = partial(get_history_full, Start = Start, End = End, Days_of_history = Days_of_history, Fresh_history = Fresh_history, API = API, Timeframe ="1H")

    Shark_pool = Pool(processes = 10)
    All_coins_history = Shark_pool.map(Partial_history, Tickers)
    Shark_pool.close()
    Shark_pool.join()

    plot = True if One_coin == 1 else False
    BTC_data = get_history_full(pair="BTC/USDT", start= Start, end= End, Days_of_history = Days_of_history, Fresh_history = Fresh_history, API = API, timeframe="1H")
    Cerebro.adddata(PandasData(dataname = BTC_data, name = "BTC/USDT", plot = not plot))

    for Coin_history in All_coins_history:
        if((Coin_history["Open"][50:].pct_change() * 100) > 500).any():
            print(f"{Coin_history['Ticker'].iloc[0]} is gapping as fuck, skip this shit")
            continue
        if Optimize:
            if len(Coin_history) > max(max(Env_period), max(ATRP_period)):
                Cerebro.adddata(PandasData(dataname = Coin_history, plot = plot, name = str(Coin_history["Ticker"].iloc[0])))
        if not Optimize:
            if len(Coin_history) > max(Env_period, ATRP_period):
                Cerebro.adddata(PandasData(dataname = Coin_history, plot = plot, name = str(Coin_history["Ticker"].iloc[0])))

    print("Got history for all the coins")

    # --------------------------------------------
    # Add observers

    Cerebro.addobserver(bt.observers.Value)
    Cerebro.addobserver(bt.observers.DrawDown)

    # --------------------------------------------
    # Add analyzers

    Cerebro.addanalyzer(bt.analyzers.DrawDown)
    Cerebro.addanalyzer(bt.analyzers.SQN)
    Cerebro.addanalyzer(Portfolioanalyzer, _name = "portfolioanalyzer")

    # --------------------------------------------
    # Add analyzers

    if (not Optimize) and (One_coin != 1):

        Cerebro.addobservermulti(MyBuySell, barplot = False, bardist = 0)
        Cerebro.addobservermulti(Position_side)

    # --------------------------------------------
    # Set our desired cash start

    Starting_cash = 10000
    Cerebro.broker.setcash(Starting_cash)
    Cerebro.broker.setcommission(commission = 0.00100, margin = False)

    # --------------------------------------------
    # Add strategy

    if Optimize:
        Cerebro.optstrategy(AB_LS_Envelope, ATRP_period = ATRP_period, Envelopes_period = Env_period, ATRP_fix_global = ATRP_fix_global)
    else:
        Cerebro.addstrategy(AB_LS_Envelope, ATRP_period = ATRP_period, Envelopes_period = Env_period, ATRP_fix_global = ATRP_fix_global)

    Results = Cerebro.run()

    Strats = [x[0] for x in Results] if Optimize else Results
    All_runs_dataframe = df()
    for i, Strat in enumerate(Strats):
        Run_data = {
        "Run number": [i],
        "End value": round(Strat.analyzers.portfolioanalyzer.get_analysis(), 2),
        "Max DD": round(Strat.analyzers.drawdown.get_analysis()['max']['drawdown'], 2),
        "SQN": round(Strat.analyzers.sqn.get_analysis()['sqn'], 5),
        "Trades": Strat.analyzers.sqn.get_analysis()['trades'],
        }
        Run_parameters = vars(Strat.params)
        Run_data.update(Run_parameters)
        print("")
        [print(f"{k}: {v}") for k, v in Run_data.items()]
        Ziobro_ty_kurwo = pd.DataFrame.from_dict(Run_data, dtype = float)
        All_runs_dataframe = All_runs_dataframe.append(Ziobro_ty_kurwo, ignore_index = True)

    if Save_excel:

        # --------------------------------------------
        # Excel save

        Basic_file_name = f"{Name}"

        if Optimize:
            Basic_file_name = f"OPTI {Basic_file_name}, {ATRP_period[0]} to {ATRP_period[-1]} ATRP, {Env_period[0]} to {Env_period[-1]} Env," \
                        f" {ATRP_fix_global[0]} to {ATRP_fix_global[-1]} Fix"
        else:
            Basic_file_name = f"{Basic_file_name}, {ATRP_period} ATRP, {Env_period} Env, {ATRP_fix_global} Fix"

        Addition_name = f"St {Start} En {End}, {datetime.now().strftime('%Y-%m-%d %H@%M')}"

        Writer = pd.ExcelWriter(f"{Basic_file_name}, {Addition_name}.xlsx", engine = "xlsxwriter")
        Workbook = Writer.book

        All_runs_dataframe.to_excel(Writer)

        # --------------------------------------------
        # Excel formatting

        Results_sheet = Writer.sheets["Sheet1"]
        Currecny_format = Workbook.add_format({'num_format': '$#,##0.00'})
        Percent_format = Workbook.add_format({'num_format': '0.00##\%'})
        Results_sheet.set_column("B:F", 12)
        Results_sheet.set_column("C:C", 12, Currecny_format)
        Results_sheet.set_column("D:D", 12, Percent_format)
        Results_sheet.set_column("F:F", 9)
        Results_sheet.set_column("G:J", 18)
        Writer.save()

    if not Optimize:
        Cerebro.plot(style = "candlestick", volume = False, barup = "limegreen", bardown = "tomato")

    print("BEEPING")
    beep(sound = 1)

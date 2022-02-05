from datetime import date, timedelta
import numpy as np
import collections

from functools import partial
from multiprocessing import Pool
import backtrader as bt


from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Refractor_general.Get_filtered_tickers import refractor_get_backtesting_tickers


# --------------------------------------------
# Tryby

Optimize = False
One_coin = True

Bot_input = False
Debugging = False

if Optimize:
    Bot_input = False

# --------------------------------------------
# Backtrader classes

class MyBuySell(bt.observers.BuySell):
    plotinfo = dict(plot = True, subplot = False, plotlinelabels = True)
    plotlines = dict(
        buy = dict(marker = "^", markersize = 10.0, color = "royalblue"),
        sell = dict(marker = "v", markersize = 10.0, color = "darkorchid")
    )

class Regreser(bt.indicators.OLS_TransformationN):
    plotlines = dict(
        spread = dict(_plotskip = True),
        spread_std = dict(_plotskip = True),
        spread_mean = dict(_plotskip = True),
    )

# --------------------------------------------
# Main strategy

class ArbStrategy(bt.Strategy):

    # --------------------------------------------
    # Parameters

    def log(self, txt, dt = None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    params = dict(
        Z_score = 0,
        )

    def __init__(self):

        # --------------------------------------------
        # Settings

        self.data0.plotinfo.plot = True
        self.Order_id = None
        self.Qty1 = 0
        self.Qty2 = 0
        self.Status = 0
        self.Upper_limit = 2.1
        self.Lower_limit = -2.1

        # --------------------------------------------
        # Indicator logic

        self.Coins_indicators = collections.defaultdict(dict)
        self.Coins = self.datas

        self.Transform = Regreser(self.data0, self.data1, plotabove = True, period = self.params.Z_score)

        self.Z_score = self.Transform.zscore
        self.Spread = self.Transform.spread

    # --------------------------------------------
    # Order check

    def notify_order(self, order):

        if order.status in [order.Completed]:

            self.Coins_indicators[order.data]["Entry_price"] = order.executed.price
            self.Coins_indicators[order.data]["Bar_executed"] = len(self)

        if Debugging:

            self.log(
                f"Order: {order.ref}\t"
                f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                f"Status {order.getstatusname()} \t"
                f"Size: {order.size:,.4f}\t"
                f"Cost: {order.size * order.created.price:,.4f}\t"
                f"Create Price: {order.created.price:,.6f}\t"
                f"Position: {self.position.size:,.4f}\t"
            )

            if order.status in [order.Margin, order.Rejected]:
                self.log(f"Order {order.ref} Margin/Rejected")

            if order.status in [order.Completed]:
                self.log(
                f"EXECUTED {order.ref}\t"
                f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                f"Price: {order.executed.price:.6f}\t"
                f"Size: {order.created.size:,.4f}\t"
                )

    # --------------------------------------------
    # Execution

    def next(self):

        if (self.Z_score[0] > self.Upper_limit) and (self.Status != 1):

            value = 0.5 * self.broker.getvalue()
            x = value / self.data0.close
            y = value / self.data1.close

            self.sell(data = self.data0, size = (x + self.Qty1))
            self.buy(data = self.data1, size = (y + self.Qty2))

            self.Qty1 = x
            self.Qty2 = y

            self.Status = 1

        elif (self.Z_score[0] < self.Lower_limit) and (self.Status != 2):

            value = 0.5 * self.broker.getvalue()
            x = value / self.data0.close
            y = value / self.data1.close

            self.buy(data = self.data0, size = (x + self.Qty1))
            self.sell(data = self.data1, size = (y + self.Qty2))

            self.Status = 2

    # --------------------------------------------
    # After run execution

    def stop(self):

        self.Portfolio = self.broker.getvalue()

        print(f"Run ${self.Portfolio:,.2f} complete")

# --------------------------------------------
# Main loop settings

def backtest_ab(API, Base, Start = date.fromisoformat("2021-03-01") , End = date.fromisoformat("2021-07-01"), Tickers_list = None):

    Delta = End - Start
    Days_of_history = Delta.days

    # --------------------------------------------
    # Filter options

    Z_score_period = tuple((50, ))

    if not Optimize:
        Z_score_period = Z_score_period[0]

    print(f"Z_score: {Z_score_period}")

    # --------------------------------------------
    # Options

    Average_z_score = np.mean(Z_score_period) if type(Z_score_period) is tuple else Z_score_period

    print(f"Optimize is: {Optimize}")

    Cerebro = bt.Cerebro(stdstats = False, optreturn = False, runonce = True, maxcpus = 6)

    # --------------------------------------------
    # Get tickers

    if One_coin:
        Tickers = ["BTC/USDT", "ETH/USDT"]
    else:
        if Tickers_list == None:
            Tickers = refractor_get_backtesting_tickers(Base = Base, API = API)
        else:
            Tickers = Tickers_list

    # --------------------------------------------
    # Add the data to Cerebro

    Start = Start - timedelta(days = max(Average_z_score, Average_z_score) / 48)

    Partial_history = partial(get_history_full, API = API, Start = Start, End = End, Days_of_history = Days_of_history, Timeframe ="30min")

    Shark_pool = Pool(processes = 6)
    All_coins_history = Shark_pool.map(Partial_history, Tickers)
    Shark_pool.close()
    Shark_pool.join()

    for Coin_history in All_coins_history:
       Cerebro.adddata(bt.feeds.PandasData(dataname = Coin_history, plot = True))

    print("Got history for all the coins")

    # --------------------------------------------
    # Change buy/sell arrows type and color

    Cerebro.addobservermulti(MyBuySell, barplot = False, bardist = 0)

    # --------------------------------------------
    # Add observers and analyzers

    Cerebro.addobserver(bt.observers.Value)
    Cerebro.addobserver(bt.observers.DrawDown)

    Cerebro.addanalyzer(bt.analyzers.Returns)
    Cerebro.addanalyzer(bt.analyzers.DrawDown)
    Cerebro.addanalyzer(bt.analyzers.SQN)

    # --------------------------------------------
    # Set our desired cash start

    Starting_cash = 10000
    Cerebro.broker.setcash(Starting_cash)
    Cerebro.broker.setcommission(commission = 0.0004, margin = False)

    # --------------------------------------------
    # Add strategy

    if Optimize:
        Cerebro.optstrategy(ArbStrategy, Z_score = Z_score_period)
    else:
        Cerebro.addstrategy(ArbStrategy, Z_score = Z_score_period)

    # --------------------------------------------
    # Run over everything

    Results = Cerebro.run()

    print(f"Max Drawdown: {Results[0].analyzers.drawdown.get_analysis()['max']['drawdown']}")
    print(f"SQN: {Results[0].analyzers.sqn.get_analysis()['sqn']}")
    print(f"Trades: {Results[0].analyzers.sqn.get_analysis()['trades']}")

    Cerebro.plot(style = "candlestick", volume = False, barup = "limegreen", bardown = "tomato")

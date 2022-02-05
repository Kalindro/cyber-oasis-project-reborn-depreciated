from datetime import date, datetime, timedelta
import time
from pandas import DataFrame as df
import pandas as pd
from beepy import beep

from functools import partial
from multiprocessing import Pool
import backtrader as bt


from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Refractor_general.Get_filtered_tickers import refractor_get_backtesting_tickers


# --------------------------------------------
# Tryby

Optimize = False
Bot_input = False
Excel_save = False

Always_in_the_market = True

One_coin = True
The_one = "BTC/USDT"

Debugging = False

# --------------------------------------------
# Filter options

Env_period = tuple((10, ))
ATRP_period = tuple((8, ))
ATRP_fix_global = tuple((10, ))
ATRP_fix_global = tuple(Number / 10 for Number in ATRP_fix_global)

if not Optimize:
    Env_period = Env_period[0]
    ATRP_period = ATRP_period[0]
    ATRP_fix_global = ATRP_fix_global[0]

print(f"Optimize is: {Optimize}")
print(f"Envelopes: {Env_period}")
print(f"ATRP: {ATRP_period}")
print(f"ATRP fix global: {ATRP_fix_global}")

# --------------------------------------------
# Backtrader classes

class MyBuySell(bt.observers.BuySell):
    plotinfo = dict(plot = True, subplot = False, plotlinelabels = True)
    plotlines = dict(
        buy = dict(marker = "^", markersize = 10.0, color = "royalblue"),
        sell = dict(marker = "v", markersize = 10.0, color = "darkorchid")
    )

class Position_side(bt.observer.Observer):
    lines = ("Position",)

    plotinfo = dict(plot = True, subplot = True)

    def next(self):
        if self._owner.position.size > 0:
            self.lines.Position[0] = 1
        elif self._owner.position.size < 0:
            self.lines.Position[0] = -1
        elif self._owner.position.size == 0:
            self.lines.Position[0] = 0

class MovingAverageSimpleEnvelope(bt.indicators.MovingAverageSimpleEnvelope):
    plotlines = dict(
        sma = dict(color = "red"),
        top = dict(color = "steelblue"),
        bot = dict(color = "steelblue"))

# --------------------------------------------
# Main strategy

class LS_Envelope(bt.Strategy):

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
        # Settings

        self.Normal_SL = False
        self.Slow_SL = False
        self.Fat_finger = False
        self.Hours = 32

        # --------------------------------------------
        # Indicator logic

        self.MA = bt.indicators.ExponentialMovingAverage(self.data.close, period = 100)
        self.ATRP_dataframe = df(columns = ["ATRP"])
        self.ATRP_fix_global = self.params.ATRP_fix_global
        self.ATRP = bt.talib.NATR(self.data_4h.high, self.data_4h.low, self.data_4h.close, timeperiod = self.params.ATRP_period, plotname = "ATRP") / 100 * self.ATRP_fix_global
        self.perc = self.ATRP * 100 / 2
        self.Envelope = MovingAverageSimpleEnvelope(self.data.close, perc = self.perc, plotname = "SMA Envelope", period = self.params.Envelopes_period)
        self.SL_execute = False

    # --------------------------------------------
    # Order check

    def notify_order(self, order):

        if order.status in [order.Completed]:

            self.Entry_price = order.executed.price
            self.bar_executed = len(self)

        if Debugging:

            self.log(
                f"Order: {order.ref}\t"
                f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                f"Status {order.getstatusname()} \t"
                f"Size: {order.size:,.4f}\t"
                f"Cost: {order.size * order.created.price:,.4f}\t"
                f"Create Price: {order.created.price:,.6f}\t"
                f"Position: {self.position.size:,.4f}\t"
                f"Money: {self.broker.getvalue():,.4f}\t"
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

        self.ATRP_amount = {"ATRP": self.ATRP[0]}
        self.ATRP_dataframe = self.ATRP_dataframe.append(self.ATRP_amount, ignore_index = True)

        # --------------------------------------------
        # Cancel all orders

        Orders = self.broker.get_orders_open()
        if Orders:
            for Order in Orders:
                self.broker.cancel(Order)

        # --------------------------------------------
        # Buys

        Size = self.broker.getvalue() * 0.95 / self.data.open

        if Always_in_the_market:

            if self.position.size > 0:
                Close_long = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.top[0])
                Open_short = self.sell(exectype = bt.Order.Limit, price = self.Envelope.lines.top[0], size = Size)

            if self.position.size < 0:
                Close_short = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.bot[0])
                Open_long = self.buy(exectype = bt.Order.Limit, price = self.Envelope.lines.bot[0], size = Size)

            if not self.position:

                if self.data.low < self.Envelope.lines.bot:
                    Open_long = self.buy(exectype = bt.Order.Stop, price = self.Envelope.lines.bot[0], size = Size)

                if self.data.high > self.Envelope.lines.top:
                    Open_short = self.sell(exectype = bt.Order.Stop, price = self.Envelope.lines.top[0], size = Size)

        if not Always_in_the_market:

            if self.position.size > 0:
                Close_long = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.top[0])

            if self.position.size < 0:
                Close_short = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.bot[0])

            if not self.position:

                if self.data.close > self.MA:
                    Open_long = self.buy(exectype = bt.Order.Limit, price = self.Envelope.lines.bot[0], size = Size)

                if self.data.close < self.MA:
                    Open_short = self.sell(exectype = bt.Order.Limit, price = self.Envelope.lines.top[0], size = Size)

    # --------------------------------------------
    # After run execution

    def stop(self):
        self.Portfolio = self.broker.getvalue()
        self.ATRP_median = self.ATRP_dataframe["ATRP"].median()

        print(f"Run ${self.Portfolio:,.2f} complete")

# --------------------------------------------
# Main loop settings

def paralityk(Tick, Start, End, API):

    try:

        Final_results_list = []
        Final_return = dict()

        # --------------------------------------------
        # Create an instance of cerebro

        print("||| COIN START |||")

        Cerebro = bt.Cerebro(stdstats = False, runonce = True, preload = True, optreturn = True, optdatas = True, maxcpus = 10)
        Pair = Tick if not One_coin else The_one

        print(f"Working on {Pair}")

        # --------------------------------------------
        # Getting pair history

        Delta = End - Start
        Days_of_history = Delta.days

        while True:
            try:
                History_dataframe_main = get_history_full(pair= Pair, start= Start, end= End, Days_of_history = Days_of_history, API = API, timeframe="1h")
            except Exception as Err:
                print(f"Lol, {Err}")
                time.sleep(2.5)
            else:
                break

        print(f"Got {Pair} history")

        Symbol = Pair[:-5] if Pair.endswith("/USDT") else Pair[:-4]

        # --------------------------------------------
        # Add the data to Cerebro

        data = bt.feeds.PandasData(dataname = History_dataframe_main, name = Pair, timeframe = bt.TimeFrame.Minutes, compression = 60)
        Cerebro.adddata(data, name = str(Pair))

        # --------------------------------------------
        # Change buy/sell arrows type and color

        Cerebro.addobserver(MyBuySell, barplot = False, bardist = 0)

        # --------------------------------------------
        # Add observers and analyzers

        Cerebro.addobserver(bt.observers.Value)
        Cerebro.addobserver(bt.observers.DrawDown)
        Cerebro.addobserver(Position_side)

        Cerebro.addanalyzer(bt.analyzers.DrawDown, _name = "Drawdown")
        Cerebro.addanalyzer(bt.analyzers.SQN, _name = "SQN")

        # --------------------------------------------
        # Set our desired cash start

        Starting_cash = 1000
        Cerebro.broker.setcash(Starting_cash)
        Cerebro.broker.setcommission(commission = 0.00075, margin = False)
        Cerebro.addsizer(bt.sizers.AllInSizer)

        # --------------------------------------------
        # Add strategy

        if Optimize:
            Cerebro.optstrategy(LS_Envelope, ATRP_period = ATRP_period, Envelopes_period = Env_period, ATRP_fix_global = ATRP_fix_global)
        else:
            Cerebro.addstrategy(LS_Envelope, ATRP_period = ATRP_period, Envelopes_period = Env_period, ATRP_fix_global = ATRP_fix_global)

        # --------------------------------------------
        # Run over everything

        Results = Cerebro.run()

        # --------------------------------------------
        # Get final run values

        for Run in Results:
            if Optimize:
                for Strategy in Run:
                    Portfolio_value = Strategy.Portfolio
                    Final_PnL = Portfolio_value - Starting_cash
                    Max_dd = Strategy.analyzers.Drawdown.get_analysis()["max"]["drawdown"] / 100
                    OnTester = Final_PnL / Max_dd if Max_dd > 0 else Final_PnL
                    ATRP_median = Strategy.ATRP_median
                    Run_env_period = Strategy.params.Envelopes_period
                    Run_ATRP_period = Strategy.params.ATRP_period
                    Run_ATRP_fix_global = Strategy.params.ATRP_fix_global
                    Run_SQN = round(Strategy.analyzers.SQN.get_analysis()["sqn"], 4)
                    Run_trades = Strategy.analyzers.SQN.get_analysis()["trades"]
                    data = {"Symbol": Symbol,
                            "Pair": Pair,
                            "Final PnL": Final_PnL,
                            "Max DD": Max_dd,
                            "OnTester": OnTester,
                            "ATRP median": ATRP_median,
                            "ENV period": Run_env_period,
                            "ATRP period": Run_ATRP_period,
                            "ATRP fix": Run_ATRP_fix_global,
                            "SQN": Run_SQN,
                            "Trades": Run_trades,
                            }
                    Final_results_list.append(data)
            else:
                Portfolio_value = Run.Portfolio
                Final_PnL = Portfolio_value - Starting_cash
                Max_dd = Run.analyzers.Drawdown.get_analysis()["max"]["drawdown"] / 100
                OnTester = Final_PnL / Max_dd if Max_dd > 0 else Final_PnL
                ATRP_median = Run.ATRP_median
                SQN = round(Run.analyzers.SQN.get_analysis()["sqn"], 4)
                Trades = Run.analyzers.SQN.get_analysis()["trades"]
                data = {"Symbol": Symbol,
                        "Pair": Pair,
                        "Final PnL": Final_PnL,
                        "Max DD": Max_dd,
                        "OnTester": OnTester,
                        "ATRP median": ATRP_median,
                        "ENV period": Env_period,
                        "ATRP period": ATRP_period,
                        "ATRP fix": ATRP_fix_global,
                        "SQN": SQN,
                        "Trades": Trades,
                        }
                Final_results_list.append(data)

        print("||| COIN END |||")

        # --------------------------------------------
        # Print out the final result

        for Result in Final_results_list:
            print(f"Symbol: {Result['Symbol']}, Pair: {Result['Pair']}, Final PnL: ${Result['Final PnL']:,.2f},"
                  f" Max DD: {Result['Max DD']:.2%}," f" OnTester: {Result['OnTester']:,.2f}, ATRP median: {Result['ATRP median']:.2%},"
                  f" ENV period: {Result['ENV period']}, ATRP period: {Result['ATRP period']}, ATRP fix: {Result['ATRP fix']},"
                  f" SQN: {Result['SQN']}, Trades: {Result['Trades']}")

        # --------------------------------------------
        # Plot results maybe

        if One_coin and not Optimize:
            Cerebro.plot(style = "candlestick", volume = False, barup = "limegreen", bardown = "tomato", end = End - timedelta(hours = 12, minutes = 0, seconds = 0.01))

        # --------------------------------------------
        # Return

        Final_return["Completed"] = Final_results_list

        return Final_return

    # --------------------------------------------
    # Loop exception handle

    except Exception as err:
        raise err

def backtest_ab(API, Base, Start = date.fromisoformat("2021-01-01") , End = date.fromisoformat("2021-03-01"), Tickers_list = None):

    try:

        Name = API["Name"]
        print(Name)

        # --------------------------------------------
        # Prepare lists and dataframe

        Results_dataframe = df(columns = ["Symbol", "Pair", "Final PnL", "Max DD", "OnTester", "ATRP median", "ENV period", "ATRP period", "ATRP fix", "SQN", "Trades"])
        Removed_tickers_short = []
        Removed_tickers_loser = []
        Removed_tickers_dumper = []
        Removed_tickers_ATRP = []

        Delta = End - Start
        Days_of_history = Delta.days

        # --------------------------------------------
        # Get tickers

        if One_coin:
            Tickers = {1}
        else:
            if Tickers_list == None:
                Tickers = refractor_get_backtesting_tickers(Base = Base, API = API)
            else:
                Tickers = Tickers_list

        # --------------------------------------------
        # Map loop through execute

        Tickers_length = len(Tickers)

        print(f"Amount of tickers: {Tickers_length}")

        Partial_paralityk = partial(paralityk, Start = Start, End = End, API = API)

        Shark_pool = Pool(processes = min(6, len(Tickers)))
        All_coins_results = Shark_pool.map(Partial_paralityk, Tickers)
        Shark_pool.close()
        Shark_pool.join()

        for Coin_results in All_coins_results:
            if Coin_results.get("Short") is not None: Removed_tickers_short.append(Coin_results.get("Short"))
            if Coin_results.get("Completed") is not None:
                for Coin_runs in Coin_results.get("Completed"):
                    Results_dataframe = Results_dataframe.append(Coin_runs, ignore_index = True)

        # --------------------------------------------
        # Excel naming

        Basic_file_name = f"{Name}, {Base} coins"

        if Optimize:
            Basic_file_name = f"OPTIMIZATION {Basic_file_name}, {Env_period[0]} to {Env_period[-1]} ENV P, {ATRP_period[0]} to {ATRP_period[-1]} ATRP P," \
                        f" {ATRP_fix_global[0]} to {ATRP_fix_global[-1]} ATRP fix"
        else:
            Basic_file_name = f"{Basic_file_name}, {Env_period} ENV P, {ATRP_period} ATRP P, {ATRP_fix_global} ATRP fix global"

        Addition_name = f"{Days_of_history} days, {datetime.now().strftime('%Y-%m-%d %H@%M')}"

        Results_dataframe.sort_values(by = ["SQN"], inplace = True, ascending = False)
        Results_dataframe.reset_index(drop = True, inplace = True)

        if Excel_save:

            # --------------------------------------------
            # Excel save

            Writer = pd.ExcelWriter(f"{Basic_file_name}, {Addition_name}.xlsx", engine = "xlsxwriter")
            Workbook = Writer.book

            if Optimize:
                for One_env_period in tuple(Env_period, ):
                    for One_atrp_period in tuple(ATRP_period, ):
                        for One_atrp_fix in tuple(ATRP_fix_global, ):
                            Dataframe_slice = Results_dataframe[(Results_dataframe["ENV period"] == One_env_period) & (
                                    Results_dataframe["ATRP period"] == One_atrp_period) & (
                                    Results_dataframe["ATRP fix"] == One_atrp_fix)]
                            Dataframe_slice.reset_index(drop = True, inplace = True)
                            if not One_coin:
                                print(f"Length before cut: {len(Dataframe_slice)}")
                                Skrajne = round(len(Dataframe_slice) * 0.025)
                                Dataframe_slice = Dataframe_slice.iloc[:-Skrajne]
                                Dataframe_slice = Dataframe_slice.iloc[Skrajne:]
                                print(f"Length after cut: {len(Dataframe_slice)}")
                            Dataframe_slice = Dataframe_slice.append(Dataframe_slice.median(), ignore_index = True)
                            Dataframe_slice.loc[Dataframe_slice.index[-1], ["Pair", "Symbol", "ENV period", "ATRP period", "ATRP fix"]] = "MEDIAN"
                            Dataframe_slice.to_excel(Writer, sheet_name = f"EV {One_env_period} AP {One_atrp_period} Fx {One_atrp_fix}")
            else:
                Results_dataframe.to_excel(Writer)

            # --------------------------------------------
            # Excel formatting

            Format_1 = Workbook.add_format({'num_format': '#,##0.00'})
            Format_2 = Workbook.add_format({'num_format': '0.00%'})
            for Sheet_name in list(Workbook.sheetnames.keys()):
                Sheet = Writer.sheets[Sheet_name]
                Results_sheet = Sheet
                Results_sheet.set_column("B:B", 10)
                Results_sheet.set_column("C:C", 13)
                Results_sheet.set_column("D:D", 13, Format_1)
                Results_sheet.set_column("E:E", 13, Format_2)
                Results_sheet.set_column("F:F", 13, Format_1)
                Results_sheet.set_column("G:G", 13, Format_2)
                Results_sheet.set_column("H:H", 13)
                Results_sheet.set_column("I:I", 13)
            Writer.save()

            print("Done, results saved")

        # --------------------------------------------
        # Beep

        print("BEEPING")
        beep(sound = 1)

        # --------------------------------------------
        # Show removed tickers

        print(f"Removed tickers in backtest because of too short history: {Removed_tickers_short}")

        if Bot_input and not One_coin:
            print(f"Removed tickers in backtest because of losing money: {Removed_tickers_loser}")
            print(f"Removed tickers in backtest because of drawdown: {Removed_tickers_dumper}")
            print(f"Removed tickers in backtest because of too big or too small ATRP: {Removed_tickers_ATRP}")
        else:
            print("Not removing any other tickers, not bot input")

        return Results_dataframe, Basic_file_name

    except Exception as err:
        raise err

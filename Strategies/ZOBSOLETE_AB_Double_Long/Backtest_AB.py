from datetime import datetime
import time
import numpy as np
import pandas as pd

from functools import partial
from multiprocessing import Pool
import backtrader as bt
from beepy import beep
from pandas import DataFrame as df

from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Refractor_general.Get_filtered_tickers import refractor_get_backtesting_tickers


# --------------------------------------------
# Filter options

Min_allowed_DD = 0.025
Max_allowed_DD = 0.5
Min_ATRP = 0.025
Max_ATRP = 0.50

# --------------------------------------------
# Tryby

Optimize = False
One_coin = False

Bot_input = False
Debugging = False

if Optimize:
    Bot_input = False

Env_period = tuple((5, ))
ATRP_period = tuple((88, ))
ATRP_fix_global = tuple((37, ))
ATRP_fix_global = tuple(Number / 10 for Number in ATRP_fix_global)

if not Optimize:
    Env_period = Env_period[0]
    ATRP_period = ATRP_period[0]
    ATRP_fix_global = ATRP_fix_global[0]

print(f"Envelopes: {Env_period}")
print(f"ATRP: {ATRP_period}")
print(f"ATRP fix global: {ATRP_fix_global}")

# --------------------------------------------
# Options

Average_env = np.mean(Env_period) if type(Env_period) is tuple else Env_period
Average_atrp = np.mean(ATRP_period) if type(ATRP_period) is tuple else ATRP_period

print(f"Optimize is: {Optimize}")

# --------------------------------------------
# Backtrader classes

class MyBuySell(bt.observers.BuySell):
    plotinfo = dict(plot = True, subplot = False, plotlinelabels = True)
    plotlines = dict(
        buy = dict(marker = "^", markersize = 10.0, color = "royalblue"),
        sell = dict(marker = "v", markersize = 10.0, color = "darkorchid")
    )

class MovingAverageSimpleEnvelope(bt.indicators.MovingAverageSimpleEnvelope):
    plotlines = dict(
        sma = dict(color = "red"),
        top = dict(color = "steelblue"),
        bot = dict(color = "steelblue"))

# --------------------------------------------
# Main strategy

class EnvelopeStrategy(bt.Strategy):

    # --------------------------------------------
    # Parameters

    def log(self, txt, dt = None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    params = (("ATRP_period", 0),
              ("Envelopes_period", 0),
              ("ATRP_fix_global", 0),
              )

    def __init__(self):

        # --------------------------------------------
        # Settings

        self.Normal_SL = True
        self.Slow_SL = True
        self.Fat_finger = False
        self.Hours = 32

        # --------------------------------------------
        # Indicator logic

        self.ATRP_dataframe = df(columns = ["ATRP"])
        self.ATRP_fix_global = self.params.ATRP_fix_global
        self.ATRP = bt.talib.NATR(self.data.high, self.data.low, self.data.close, timeperiod = self.params.ATRP_period, plotname = "ATRP") / 100 * self.ATRP_fix_global
        self.perc = self.ATRP * 100 / 2
        self.Envelope = MovingAverageSimpleEnvelope(self.data.close, perc = self.perc, plotname = "SMA Envelope", period = self.params.Envelopes_period)
        self.perc_nominal = self.perc / 100 * self.Envelope.lines.sma
        self.SL_execute = False
        self.Dumped = False

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
                f"Size left: {self.Size_left}"
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

        if not self.position:

            Buy_order1 = self.buy(exectype = bt.Order.Limit, price = self.Envelope.lines.bot[0])

        if self.position:

            self.duration = len(self) - self.bar_executed + 1

            if self.Fat_finger:
                if ((min(self.data.close, self.data.open) / self.data.low) - 1) > (self.ATRP / 2) and not self.SL_execute:
                    Sell_order = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.sma[0])
                    self.SL_execute = True

            if self.Normal_SL:
                if (self.duration > self.Hours) and (self.data.low < self.Entry_price) and not self.SL_execute:
                    Sell_order = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.sma[0])
                    self.SL_execute = True

            if self.Slow_SL:
                if (self.duration > self.Hours * 2) and not self.SL_execute:
                    Sell_order = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.sma[0])
                    self.SL_execute = True

            if not self.SL_execute:
                Sell_order = self.close(exectype = bt.Order.Limit, price = self.Envelope.lines.top[0])

            self.SL_execute = False

    # --------------------------------------------
    # After run execution

    def stop(self):
        self.Portfolio = self.broker.getvalue()
        self.ATRP_median = self.ATRP_dataframe["ATRP"].median()

        print(f"Run ${self.Portfolio:,.2f} complete")

# --------------------------------------------
# Main loop settings

def paralityk(Tick, API, Desired_days):

    try:

        # --------------------------------------------
        # Create an instance of cerebro

        print("||| COIN START |||")

        Cerebro = bt.Cerebro(stdstats = False, optreturn = False, runonce = False, maxcpus = 1)
        Pair = Tick if not One_coin else "IDEA/USDT"

        print(f"Working on {Pair}")

        # --------------------------------------------
        # Getting pair history

        Days_of_history = Desired_days + max(Average_env, Average_atrp) / 24

        while True:
            try:
                History_dataframe_main = get_history_full(API, Days_of_history, Pair, timeframe="1h")
            except Exception as Err:
                print(f"Lol, {Err}")
                time.sleep(2.5)
            else:
                break

        print(f"Got {Pair} history")

        if len(History_dataframe_main) < (Desired_days / 2.5 * 24):
            print(f"{Pair} is too short, skipping")
            return [None, Pair]

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

        Cerebro.addanalyzer(bt.analyzers.DrawDown, _name = "Drawdown")
        Cerebro.addanalyzer(bt.analyzers.SQN, _name = "SQN")

        # --------------------------------------------
        # Set our desired cash start

        Starting_cash = 1000
        Cerebro.broker.setcash(Starting_cash)
        Cerebro.broker.setcommission(commission = 0.00125, margin = False)
        Cerebro.addsizer(bt.sizers.AllInSizer)

        # --------------------------------------------
        # Add strategy

        if Optimize:
            Cerebro.optstrategy(EnvelopeStrategy, ATRP_period = ATRP_period, Envelopes_period = Env_period, ATRP_fix_global = ATRP_fix_global)
        else:
            Cerebro.addstrategy(EnvelopeStrategy, ATRP_period = ATRP_period, Envelopes_period = Env_period, ATRP_fix_global = ATRP_fix_global)

        # --------------------------------------------
        # Run over everything

        Results = Cerebro.run()

        # --------------------------------------------
        # Get final run values

        Final_results_list = []
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
            Cerebro.plot(style = "candlestick", volume = False, barup = "limegreen", bardown = "tomato")

        # --------------------------------------------
        # Return

        return [Final_results_list, None]

    # --------------------------------------------
    # Loop exception handle

    except Exception as err:
        raise err


def backtest_ab(API, Base, Desired_days = 120, Tickers_list = None):

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

        # --------------------------------------------
        # Get tickers

        if One_coin:
            Tickers = {1}
        else:
            if Tickers_list == None:
                Tickers = refractor_get_backtesting_tickers(API, Base)
            else:
                Tickers = Tickers_list

        # --------------------------------------------
        # Map loop through execute

        Tickers_length = len(Tickers)

        print(f"Amount of tickers: {Tickers_length}")

        Partial_func = partial(paralityk, API = API, Desired_days = Desired_days)

        if "GATEIO" in Name.upper():
            All_coins_results = map(Partial_func, Tickers)
        else:
            Shark_pool = Pool(processes = 8)
            All_coins_results = Shark_pool.map(Partial_func, Tickers)
            Shark_pool.close()
            Shark_pool.join()

        for Coin_results in All_coins_results:
            if Coin_results[1] is not None:
                Removed_tickers_short.append(Coin_results[1])
            if Coin_results[0] is not None:
                for Coin_runs in Coin_results[0]:
                    Results_dataframe = Results_dataframe.append(Coin_runs, ignore_index = True)

        # --------------------------------------------
        # Excel save

        Basic_file_name = f"{Name}, {Base} coins"

        if Optimize:
            Basic_file_name = f"OPTIMIZATION {Basic_file_name}, {Env_period[0]} to {Env_period[-1]} ENV P, {ATRP_period[0]} to {ATRP_period[-1]} ATRP P," \
                        f" {ATRP_fix_global[0]} to {ATRP_fix_global[-1]} ATRP fix"
        else:
            Basic_file_name = f"{Basic_file_name}, {Env_period} ENV P, {ATRP_period} ATRP P, {ATRP_fix_global} ATRP fix global"

        Addition_name = f"{Desired_days} days, {datetime.now().strftime('%Y-%m-%d %H@%M')}"

        # --------------------------------------------
        # Remove pairs that lose money, are above allowed drawdown or ATRP is too small or to big

        if Bot_input and not Optimize:

            for Pair_number, Row in Results_dataframe.iterrows():
                if Row["OnTester"] <= 0 or Row["Final PnL"] <= 0 or Row["SQN"] <= 0.5 or Row["Trades"] <= 10:
                    Removed_tickers_loser.append(Row["Pair"])
                    Results_dataframe.drop(Pair_number, inplace = True)
                    continue

                if not Min_allowed_DD < Row["Max DD"] < Max_allowed_DD:
                    Removed_tickers_dumper.append(Row["Pair"])
                    Results_dataframe.drop(Pair_number, inplace = True)
                    continue

                if not Min_ATRP < Row["ATRP median"] < Max_ATRP:
                    Removed_tickers_ATRP.append(Row["Pair"])
                    Results_dataframe.drop(Pair_number, inplace = True)
                    continue

        Results_dataframe.sort_values(by = ["SQN"], inplace = True, ascending = False)
        Results_dataframe.reset_index(drop = True, inplace = True)

        # --------------------------------------------
        # Excel name and save

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

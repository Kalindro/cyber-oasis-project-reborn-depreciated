import backtrader as bt


class CommInfoFractional(bt.CommissionInfo):
    def getsize(self, price, cash):
        return self.p.leverage * (cash / price)


class PortfolioAnalyzer(bt.Analyzer):
    def next(self):
        self.portfolio_ending = self.strategy.broker.get_value()

    def get_analysis(self):
        return self.portfolio_ending


class ExposureObserver(bt.Observer):
    alias = ("ExposureObserver",)
    lines = ("exposure",)

    plotinfo = dict(plot=True, subplot=True)

    def next(self):
        value = self._owner.broker.getvalue()
        self.lines.exposure[0] = round((1 - self._owner.broker.getcash() / value) * 100, 2)


class MyBuySell(bt.observers.BuySell):
    plotinfo = dict(plot=True, subplot=False, plotlinelabels=True)
    plotlines = dict(
        buy=dict(marker="^", markersize=10.0, color="royalblue"),
        sell=dict(marker="v", markersize=10.0, color="darkorchid")
    )


class AddAnalyzers:
    def __init__(self, cerebro):
        self.cerebro = cerebro

    def add_analyzers(self):
        # Add observers
        self.cerebro.addobserver(bt.observers.Value)
        self.cerebro.addobserver(bt.observers.DrawDown)
        self.cerebro.addobservermulti(MyBuySell, barplot=False, bardist=0)
        self.cerebro.addobserver(ExposureObserver, _name="exposure_observer")

        # Add analyzers
        self.cerebro.addanalyzer(bt.analyzers.DrawDown)
        self.cerebro.addanalyzer(bt.analyzers.SQN)
        self.cerebro.addanalyzer(PortfolioAnalyzer, _name="portfolio_analyzer")

        # Set our desired cash start
        starting_cash = 10_000
        self.cerebro.broker.setcash(starting_cash)
        self.cerebro.broker.setcommission(commission=0.00100)
        self.cerebro.broker.addcommissioninfo(CommInfoFractional())
        self.cerebro.broker.set_checksubmit(False)

        return self.cerebro

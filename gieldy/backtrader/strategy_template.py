import backtrader as bt


class StandardStrategy(bt.Strategy):
    """This is a standard strategy. Each ``strategy`` class will inherit from here"""

    def log(self, txt):
        """Logging function"""
        dt = self.datas[0].datetime.datetime(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def notify_order(self, order):
        """Loging for new orders"""

        # Base logging for all the cases
        base_logging = (f"Type: {('BUY' if order.isbuy() else 'SELL')}\t"
                        f"Status: {order.getstatusname()}\t"
                        f"Order size: {order.created.size:,.4f}\t"
                        f"Create price: {order.executed.price:,.4f}\t"
                        f"Order cost: {order.executed.value:,.2f}\t"
                        f"Comm: {order.executed.comm:,.4f}, "
                        f"Cash: {self.broker.get_cash():,.2f}\t"
                        f"Acc Value: {self.broker.get_value():,.2f}\t"
                        f"Position: {self.getposition(order.data).size:,.4f}\t"
                        f"Coin: {order.data._name}\t"
                        )

        # Suppress notification if it is just a submitted order.
        # if order.status == order.Submitted:
        #     return

        if order.status in [order.Margin, order.Rejected]:
            self.log(f"Order {order.ref} Margin/Rejected\t" + base_logging)

        if self.p.print_orders_trades:
            self.log(base_logging)

    def notify_trade(self, trade):
        """Provides notification of closed trades"""
        if trade.isclosed:
            if self.p.print_orders_trades:
                self.log(
                    f"PNL: ${trade.pnlcomm:.2f}\t"
                    f"Coin: {trade.data._name}\t"
                    )
            else:
                pass

    def print_signal(self, dataline):
        """Print out OHLCV"""
        self.log(
            "o {:5.2f}\th {:5.2f}\tl {:5.2f}\tc {:5.2f}\tv {:5.0f}".format(
                self.datas[dataline].open[0],
                self.datas[dataline].high[0],
                self.datas[dataline].low[0],
                self.datas[dataline].close[0],
                self.datas[dataline].volume[0],
            )
        )

    def print_dev(self):
        """For development logging"""
        self.log(
            f"Value: {self.broker.cash:5.2f}, "
            f"Cash: {self.broker.get_value():5.2f}, "
            f"Close:{self.datas[1].close[0]:5.2f}, "
        )

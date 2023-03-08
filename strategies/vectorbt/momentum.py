from prime_functions.momentums import momentum_ranking_with_parity


def momentum_strat():

    allocations = momentum_ranking_with_parity()

    prices = pd.read_csv('prices.csv', index_col='datetime')

    # Define parameters
    cash = 1000000  # starting cash

    # Compute the position sizes
    size = allocations * cash

    # Create orders dataframe
    orders = size.diff().fillna(0)

    # Create portfolio
    portfolio = vbt.Portfolio.from_orders(orders, price=prices)

    # Run backtest
    portfolio.run()

    # Analyze results
    total_return = portfolio.total_return()
    sharpe_ratio = portfolio.sharpe_ratio()
    max_drawdown = portfolio.max_drawdown()
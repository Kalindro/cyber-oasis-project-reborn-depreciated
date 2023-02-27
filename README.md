# CyberOasisProjectReborn

My main repository containing all the functions, utilities, strategies that help me to trade & invest on crypto
markets.

This project allowed me to self-develop my Python skills. I've started with minimal programing knowledge, where now I'm
able to create tools that have real use.

Most useful scripts are:

- GetFullHistoryDF().main in generic.get_full_history.py - Allows to query desired pair OHLCV history (timeframe, desired history range, last N candles, save/load option)
- strategies.market_performers.market_performers.py - Ouptut an excel file with few metrics that can help analyze the top performing coins
- All scripts in strategies.manual_utility - Allow to change leverage on all pairs, calculate beta neutral parity, calculate risk parity split of portfolio based on Clenow's "Stocks on the Move" book (allocation that depends on the pair volatality)

The user required inputs are exchange API keys, .env file in API folder should have structure as the example
(env_template).

# CyberOasisProjectReborn

My main repository containing all the functions, utilities, strategies that help me to navigate on crypto markets.
Please note it's a repo I constantly work on, a lot of functions are WIP and the structure changes almost daily.

The user required inputs to invoke exchange instance are exchange API keys. They should be in the `config.yaml`
file, the same structure as the `config_template.yaml`.

Most useful scripts are:

- `CEFI.exchange.exchanges`: 
Allows to invoke chosen instance of exchange using CCXT. The invoked class allows to access many custom functions
using Exchange.functions(). Those methods include querying OHLCV history (timeframe, desired history range,
last N candles, save/load option), exchange pairs list, changing leverage. Original CCXT methods are accessible
with Exchange.exchange_client. 
- `scripts.market_performers.market_performers.py` - Outputs an excel file with few metrics that can help analyze the 
top performing coins.
- Many old utilities are depreciated due to my core change in approach of handling history data,
they will become usable again.
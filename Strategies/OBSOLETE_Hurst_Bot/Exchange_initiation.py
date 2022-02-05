from configparser import ConfigParser

import ccxt

def exchange_initiation():

    parser = ConfigParser()
    parser.read("Binance.ini")

    Public_key = parser.get("Trade_keys", "Public_key")
    Secret_key = parser.get("Trade_keys", "Secret_key")

    Exchange_id = "binance"
    Exchange_class = getattr(ccxt, Exchange_id)
    Exchange = Exchange_class({
        "apiKey": Public_key,
        "secret": Secret_key,
        "timeout": 30000,
        "enableRateLimit": True
    })
    return Exchange

def name():
    Name = "Binance Hurst"

    return Name
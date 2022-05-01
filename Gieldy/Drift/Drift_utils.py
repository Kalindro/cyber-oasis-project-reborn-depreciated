from typing import Optional, TypeVar, Type, cast
from driftpy.types import (
    PositionDirection,
    StateAccount,
    MarketsAccount,
    FundingPaymentHistoryAccount,
    FundingRateHistoryAccount,
    TradeHistoryAccount,
    LiquidationHistoryAccount,
    DepositHistoryAccount,
    ExtendedCurveHistoryAccount,
    User,
    UserPositions,
)
from driftpy.constants.markets import MARKETS
from driftpy.constants.numeric_constants import MARK_PRICE_PRECISION, QUOTE_PRECISION
from driftpy.clearing_house_user import ClearingHouseUser
import asyncio

from datetime import datetime, timedelta
from pythclient.pythaccounts import PythPriceAccount
from pythclient.solana import (SolanaClient, SolanaPublicKey, SOLANA_DEVNET_HTTP_ENDPOINT, SOLANA_DEVNET_WS_ENDPOINT,
                               SOLANA_MAINNET_HTTP_ENDPOINT, SOLANA_MAINNET_HTTP_ENDPOINT)

import pandas as pd
import numpy as np


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


AMM_PRECISION = 1e13
FUNDING_PRECISION = 1e4


async def drift_get_margin_account_info(API):
    margin_info = dict()
    margin_info["collateral"] = await API["drift_user"].get_collateral()/QUOTE_PRECISION

    return margin_info


async def drift_get_pair_prices_rates(API):
    markets = await API["drift_acct"].get_markets_account()
    market_summary = await drift_calculate_market_summary(markets)
    return market_summary


async def drift_load_positions(API):
    async def async_apply(ser, lambd):
        prices = []
        for x in ser.values.tolist():
            price = await lambd(x)
            prices.append(price)
        return prices

    position_acct = await (API["drift_acct"].program.account["UserPositions"].fetch(
        API["drift_user_acct"].positions
    ))

    # load user positions
    positions = cast(
        UserPositions,
        position_acct,
    )
    positions_df = pd.DataFrame(positions.positions)[["market_index", "base_asset_amount", "quote_asset_amount"]]
    positions_df["base_asset_amount"] /= AMM_PRECISION
    positions_df["quote_asset_amount"] /= QUOTE_PRECISION
    positions_df = pd.DataFrame(MARKETS)[["symbol", "market_index"]].merge(positions_df)
    positions_df.loc[positions_df.base_asset_amount == 0, :] = np.nan
    positions_df = positions_df[positions_df["market_index"].notna()]
    positions_df["notional"] = np.array(await (async_apply(positions_df["market_index"], API["drift_user"].get_position_value))) / QUOTE_PRECISION
    positions_df["entry_price"] = (positions_df["quote_asset_amount"] / positions_df["base_asset_amount"]).abs()
    positions_df["exit_price"] = (positions_df["notional"] / positions_df["base_asset_amount"]).abs()
    positions_df["pnl"] = (positions_df["notional"] - positions_df["quote_asset_amount"]) * positions_df["base_asset_amount"].pipe(np.sign)
    positions_df["pair"] = positions_df["symbol"]
    positions_df = positions_df[positions_df["symbol"].notna()]
    positions_df["symbol"] = positions_df["symbol"].str[:-5]
    positions_df.set_index("symbol", inplace=True)

    return positions_df


async def drift_calculate_market_summary(markets):
    markets_summary = pd.concat([
        pd.DataFrame(MARKETS).iloc[:, :3],
        pd.DataFrame(markets.markets),
        pd.DataFrame([x.amm for x in markets.markets]),
    ], axis=1).dropna(subset=["symbol"])
    last_funding_ts = pd.to_datetime(markets.markets[0].amm.last_funding_rate_ts * 1e9)
    next_funding_ts = last_funding_ts + timedelta(hours=1)

    summary = dict()

    next_funding = (markets_summary["last_mark_price_twap"] - markets_summary["last_oracle_price_twap"]) / 24
    summary["funding_rate"] = next_funding / markets_summary["last_oracle_price_twap"] * 100
    summary["funding_rate_APR"] = (summary["funding_rate"] * 24 * 365.25).round(2)
    summary["mark_price"] = (markets_summary["quote_asset_reserve"] / markets_summary["base_asset_reserve"]) * markets_summary["peg_multiplier"] / 1e3

    df = pd.concat([pd.DataFrame(MARKETS).iloc[:, :3], pd.DataFrame(summary)], axis=1)
    df = df.apply(pd.to_numeric, errors="ignore")

    df.rename(columns={"symbol": "pair", "base_asset_symbol": "symbol"}, inplace=True)
    df["pair"] = df["symbol"] + "/USDT"
    df.set_index("symbol", inplace=True)

    return df


async def drift_open_market_long(API, amount, drift_index):
    open_position = await API["drift_acct"].open_position(direction=PositionDirection.LONG(), amount=int(amount), market_index=int(drift_index))

    return open_position


async def drift_open_market_short(API, amount, drift_index):
    open_position = await API["drift_acct"].open_position(direction=PositionDirection.SHORT(), amount=int(amount), market_index=int(drift_index))

    return open_position


async def drift_close_order(API, drift_index):
    close_position = await API["drift_acct"].close_position(market_index=drift_index)

    return close_position

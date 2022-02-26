
import asyncio
import os
import pandas as pd

from Gieldy.Drift.Drift_utils import load_position_table, calculate_market_summary


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


async def get_margin_account_info(self):
    account = await self.accinit()
    margin_info = dict()
    margin_info['total_collateral'] = await account["drift_user"].get_total_collateral()/self.QUOTE_PRECISION
    margin_info['unrealised_pnl'] = await account["drift_user"].get_unrealised_pnl(0)/self.QUOTE_PRECISION
    margin_info['leverage'] = await account["drift_user"].get_leverage()
    margin_info['free_collateral'] = await account["drift_user"].get_free_collateral()/self.QUOTE_PRECISION
    return margin_info


async def markets_summary(self):
    account = await self.accinit()
    markets = await account["drift_acct"].get_markets_account()
    market_summary = await calculate_market_summary(markets)
    print(market_summary)

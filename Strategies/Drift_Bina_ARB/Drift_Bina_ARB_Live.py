from driftpy.clearing_house_user import ClearingHouse, ClearingHouseUser
from pathlib import Path
import asyncio
import os

from Gieldy.Refractor_general.Arb_helpers import load_position_table


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class DriftBinaARBLive:

    def __init__(self):
        current_path = os.path.dirname(os.path.abspath(__file__))
        project_path = Path(current_path).parent.parent
        os.environ["ANCHOR_WALLET"] = f"{project_path}\Gieldy\APIs\Solana_Drift_Bina_ARB.json"
        self.QUOTE_PRECISION = 1e6

    async def accinit(self):
        user_account = dict()
        user_account["drift_acct"] = await ClearingHouse.create_from_env("mainnet")
        user_account["drift_user"] = ClearingHouseUser(user_account["drift_acct"], user_account["drift_acct"].program.provider.wallet.public_key)
        user_account["drift_user_acct"] = await user_account["drift_user"].get_user_account()
        return user_account

    async def get_margin_account_info(self):
        account = await self.accinit()
        margin_info = dict()
        margin_info['total_collateral'] = await account["drift_user"].get_total_collateral()/self.QUOTE_PRECISION
        margin_info['unrealised_pnl'] = await account["drift_user"].get_unrealised_pnl(0)/self.QUOTE_PRECISION
        margin_info['leverage'] = await account["drift_user"].get_leverage()
        margin_info['free_collateral'] = await account["drift_user"].get_free_collateral()/self.QUOTE_PRECISION
        print(margin_info)
        return margin_info


asyncio.run(DriftBinaARBLive().get_margin_account_info())


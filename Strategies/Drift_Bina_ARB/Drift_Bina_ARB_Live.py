from driftpy.clearing_house_user import ClearingHouse, ClearingHouseUser
from pathlib import Path
from solana import publickey
from solana import keypair
import asyncio
import os


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent
os.environ["ANCHOR_WALLET"] = f"{project_path}\Gieldy\APIs\Solana_Drift_Bina_ARB.json"


async def accinit():
    drift_acct = await ClearingHouse.create_from_env("mainnet")
    await drift_acct.initialize_user_account_and_deposit_collateral(amount=1,collateral_account_public_key="FHL6i18MNUoLhsMqwUFQfQLyVQy4R1jhzxG95NeKBkec")
    # drift_user = ClearingHouseUser(drift_acct, drift_acct.program.provider.wallet.public_key)
    # drift_user_acct = await drift_user.get_user_account()
    # print(drift_user_acct)
asyncio.run(accinit())

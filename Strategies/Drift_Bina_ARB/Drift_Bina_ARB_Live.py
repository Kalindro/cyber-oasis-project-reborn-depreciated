from driftpy import program
from driftpy.clearing_house_user import ClearingHouse, ClearingHouseUser
import asyncio
import os


dir_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(dir_path)

os.environ["ANCHOR_WALLET"] = f"{parent_path}\APIs\Solana_Drift_Bina_ARB.json"


async def accinit():
    drift_acct = await ClearingHouse.create_from_env("mainnet")
    return drift_acct

asyncio.run(accinit())

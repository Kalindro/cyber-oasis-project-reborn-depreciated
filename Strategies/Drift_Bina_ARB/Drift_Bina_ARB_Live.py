from driftpy.clearing_house_user import ClearingHouse, ClearingHouseUser
from pathlib import Path
import asyncio
import os


current_path = os.getcwd()
project_path = Path(current_path).parent.parent

os.environ["ANCHOR_WALLET"] = f"{project_path}\Gieldy\APIs\Solana_Drift_Bina_ARB.json"


async def accinit():
    drift_acct = await ClearingHouse.create_from_env("mainnet")
    return drift_acct

asyncio.run(accinit())

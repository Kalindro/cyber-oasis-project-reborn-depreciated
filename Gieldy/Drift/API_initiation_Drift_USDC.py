from driftpy.clearing_house_user import ClearingHouse, ClearingHouseUser
from pathlib import Path
import asyncio
import os

import warnings
import sys


if not sys.warnoptions:
    warnings.simplefilter("ignore")

name = "Drift USDC"

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent
os.environ["ANCHOR_WALLET"] = f"{project_path}\Gieldy\APIs\Solana_Drift_Bina_ARB.json"


async def API_initiation():
    user_account = dict()
    user_account["drift_acct"] = await ClearingHouse.create_from_env("mainnet")
    user_account["drift_user"] = ClearingHouseUser(user_account["drift_acct"], user_account["drift_acct"].program.provider.wallet.public_key)
    user_account["drift_user_acct"] = await user_account["drift_user"].get_user_account()

    API = {"name": name,
           "drift_acct":  user_account["drift_acct"],
           "drift_user":  user_account["drift_user"],
           "drift_user_acct":  user_account["drift_user_acct"]
           }

    return API

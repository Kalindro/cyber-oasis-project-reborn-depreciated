from driftpy.clearing_house_user import ClearingHouse, ClearingHouseUser
from Gieldy.Drift.Node_loader import my_load_program
from pathlib import Path
import asyncio
import os

import warnings
import sys


# if not sys.warnoptions:
#     warnings.simplefilter("ignore")

name = "Drift USDC ARB Layer 1"

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent
os.environ["ANCHOR_WALLET"] = f"{project_path}/Gieldy/APIs/Solana_Drift_ARB_Layer_1.json"


async def API_initiation(private: bool):
    user_account = dict()
    user_account["drift_acct"] = await ClearingHouse.create(program=my_load_program(private=private))
    user_account["drift_user"] = ClearingHouseUser(user_account["drift_acct"], user_account["drift_acct"].program.provider.wallet.public_key)
    user_account["drift_user_acct"] = await user_account["drift_user"].get_user_account()

    API = {"name": name,
           "drift_acct":  user_account["drift_acct"],
           "drift_user":  user_account["drift_user"],
           "drift_user_acct":  user_account["drift_user_acct"]
           }

    return API

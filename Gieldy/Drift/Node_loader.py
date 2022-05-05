import os

from solana.publickey import PublicKey
from anchorpy import Idl, Program, Provider


CONFIG = {
    "mainnet_private": {
        "ENV": "mainnet-beta",
        "URL": "https://api.mainnet-beta.solana.com/",
        "PYTH_ORACLE_MAPPING_ADDRESS": "AHtgzX45WTKfkPG53L6WYhGEXwQkN1BVknET3sVsLL8J",
        "CLEARING_HOUSE_PROGRAM_ID": "dammHkt7jmytvbS3nHTxQNEcP59aE57nxwV21YdqEDN",
        "USDC_MINT_ADDRESS": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    },
    "mainnet_public": {
        "ENV": "mainnet-beta",
        "URL": "https://api.mainnet-beta.solana.com/",
        "PYTH_ORACLE_MAPPING_ADDRESS": "AHtgzX45WTKfkPG53L6WYhGEXwQkN1BVknET3sVsLL8J",
        "CLEARING_HOUSE_PROGRAM_ID": "dammHkt7jmytvbS3nHTxQNEcP59aE57nxwV21YdqEDN",
        "USDC_MINT_ADDRESS": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    },
}


def my_load_program(private, wallet_path=None):
    if private:
        env = "mainnet_private"
    else:
        env = "mainnet_public"
    assert env in CONFIG.keys()
    CH_PID = CONFIG[env]["CLEARING_HOUSE_PROGRAM_ID"]
    IDL_JSON = None
    IDL_URL = CONFIG[env].get("IDL_URL", None)
    if IDL_URL is None:
        from driftpy.clearing_house import ClearingHouse

        IDL_JSON = ClearingHouse.local_idl()
    else:
        import requests

        print("requesting idl from", IDL_URL)
        IDL_JSON = Idl.from_json(requests.request("GET", IDL_URL).json())

    if "ANCHOR_PROVIDER_URL" not in os.environ:
        if CONFIG[env].get("URL") is not None:
            os.environ["ANCHOR_PROVIDER_URL"] = CONFIG[env]["URL"]

    p = None
    if wallet_path is not None:
        wallet_path_full = os.path.expanduser(wallet_path)
        assert os.path.exists(wallet_path_full)
        os.environ["ANCHOR_WALLET"] = wallet_path_full
        p = Provider.env()
    else:
        if "ANCHOR_WALLET" not in os.environ:
            print("No solana wallet specified/found. Read-Only mode.")
            p = Provider.readonly(url=os.environ["ANCHOR_PROVIDER_URL"])
        else:
            p = Provider.env()

    program = Program(IDL_JSON, PublicKey(CH_PID), p)
    return program

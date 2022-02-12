import asyncio
from solana.keypair import Keypair
from solana.system_program import SYS_PROGRAM_ID
from anchorpy import create_workspace, close_workspace, Context

async def main():
    # Read the deployed program from the workspace.
    workspace = create_workspace()
    print(workspace)
    # The program to execute.
    program = workspace["basic_1"]
    # The Account to create.
    my_account = Keypair()
    # Execute the RPC.
    accounts = {
        "my_account": my_account.public_key,
        "user": program.provider.wallet.public_key,
        "system_program": SYS_PROGRAM_ID
    }
    await program.rpc["initialize"](1234, ctx=Context(accounts=accounts, signers=[my_account]))
    # Close all HTTP clients in the workspace, otherwise we get warnings.
    await close_workspace(workspace)

asyncio.run(main())
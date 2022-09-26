import requests
from prefect import get_run_logger, task
from prefect.blocks.system import String
from substrateinterface import SubstrateInterface

from common.utils import dot_wei_to_ether


@task(name="DOT Balance Task")
def dot_get_balance(address: str):
    logger = get_run_logger()
    logger.info("DOT - Getting Balance")

    url = String.load("dot-rpc-url").value
    substrate = SubstrateInterface(url=url)
    result = substrate.query(
        module="System",
        storage_function="Account",
        params=[address],
    )
    total_balance = str(result["data"]["free"])
    total_balance = dot_wei_to_ether(total_balance)

    logger.info(f"DOT - Balance: {total_balance}")

    return total_balance


@task(name="DOT Staked Balance Task")
def dot_get_staked_balance(address: str):
    logger = get_run_logger()
    logger.info("DOT - Getting Staked Balance")

    url = String.load("dot-rpc-url").value
    substrate = SubstrateInterface(url=url)
    result = substrate.query(
        module="Staking",
        storage_function="Ledger",
        params=[address],
    )
    staked_balance = str(result["active"])
    staked_balance = dot_wei_to_ether(staked_balance)

    logger.info(f"DOT - Staked Balance: {staked_balance}")

    return staked_balance


@task(name="DOT Total Reward Task")
def dot_get_rewards(address: str):
    logger = get_run_logger()
    logger.info("DOT - Getting Total Rewards")

    url = "https://polkadot.api.subscan.io/api/scan/account/reward_slash"
    data = {
        "row": 100,
        "page": 0,
        "address": address,
    }
    headers = {
        "Content-Type": "application/json",
    }

    r = requests.post(url, json=data, headers=headers)
    rewards_data = r.json()

    rewards = rewards_data["data"]["list"]
    total_reward_amount = 0
    for reward in rewards:
        total_reward_amount += dot_wei_to_ether(reward["amount"])

    logger.info(f"DOT - Total Rewards: {total_reward_amount}")

    return total_reward_amount

from prefect import flow, get_run_logger, task
from prefect.blocks.system import String
from substrateinterface import SubstrateInterface


@task
def get_total_balance(address: str):
    url = String.load("dot-rpc-url").value
    substrate = SubstrateInterface(url=url)
    result = substrate.query(
        module="System",
        storage_function="Account",
        params=[address],
    )
    total_balance = str(result["data"]["free"])

    return total_balance


@task
def get_staked_balance(address: str):
    url = String.load("dot-rpc-url").value
    substrate = SubstrateInterface(url=url)
    result = substrate.query(
        module="Staking",
        storage_function="Ledger",
        params=[address],
    )
    staked_balance = str(result["active"])

    return staked_balance


@task
def wei_to_ether(wei: str):
    ether_value = round(float(wei) / 1e10, 3)
    return ether_value


@flow(name="Ledger DOT Balance Flow")
def dot_balance_flow(address: str):
    logger = get_run_logger()

    total_balance = get_total_balance(address)
    staked_balance = get_staked_balance(address)
    total_balance_ether = wei_to_ether(total_balance)
    staked_balance_ether = wei_to_ether(staked_balance)

    logger.info(f"Ledger - Total balance (DOT): {total_balance_ether}")
    logger.info(f"Ledger - Staked balance (DOT): {staked_balance_ether}")
    return total_balance_ether, staked_balance_ether


if __name__ == "__main__":
    dot_address = String.load("dot-address").value
    dot_balance_flow(dot_address)

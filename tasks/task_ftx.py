from prefect import get_run_logger, task
from prefect.blocks.system import Secret, String

from common.ftx_client import FtxClient


@task(name="FTX DOT Cost Task")
def ftx_get_dot_cost():
    logger = get_run_logger()
    logger.info("FTX - Getting DOT Cost")

    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    orders = ftx.get_order_history()
    total_cost: float = 0
    total_size: float = 0
    for order in orders:
        if (
            order["market"] == "DOT/USD"
            and order["side"] == "buy"
            and order["status"] == "closed"
        ):
            avg_price = order["avgFillPrice"]
            size = order["filledSize"]
            total_cost += avg_price * size
            total_size += size

    avg_cost = total_cost / total_size

    logger.info(f"FTX - DOT Size: {total_size}")
    logger.info(f"FTX - DOT Avg Cost: {avg_cost}")

    return total_size, avg_cost


@task(name="FTX DOT Market Price Task")
def ftx_get_dot_market_price():
    logger = get_run_logger()
    logger.info("FTX - Getting DOT Market Price")

    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    dot_market = ftx.get_market("DOT/USD")
    market_price: float = dot_market["price"]

    logger.info(f"FTX - DOT Market Price: {market_price}")

    return market_price


@task(name="FTX DOT Withdrawn Task")
def ftx_get_dot_withdrawn():
    logger = get_run_logger()
    logger.info("FTX - Getting DOT Withdrawn")

    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    dot_address = String.load("dot-address").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    withdrawals = ftx.get_withdrawals()

    total_withdrawn = 0
    for withdrawal in withdrawals:
        if withdrawal["coin"] == "DOT" and withdrawal["address"] == dot_address:
            total_withdrawn += withdrawal["size"]

    logger.info(f"FTX - DOT Withdrawn: {total_withdrawn}")

    return total_withdrawn

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
    total_usd: float = 0
    total_size: float = 0
    for order in orders:
        if (
            order["market"] == "DOT/USD"
            and order["side"] == "buy"
            and order["status"] == "closed"
        ):
            avg_price = order["avgFillPrice"]
            size = order["filledSize"]
            total_usd += avg_price * size
            total_size += size

    if total_size == 0:
        avg_cost = 0
    else:
        avg_cost = total_usd / total_size

    logger.info(f"FTX - DOT Size: {total_size}")
    logger.info(f"FTX - DOT Avg Price: {avg_cost}")
    logger.info(f"FTX - DOT Cost USD: {total_usd}")

    return total_size, avg_cost


@task(name="FTX DOT Settlement Task")
def ftx_get_dot_settlement():
    logger = get_run_logger()
    logger.info("FTX - Getting DOT Settlement")

    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    orders = ftx.get_order_history()
    total_usd: float = 0
    total_size: float = 0
    for order in orders:
        if (
            order["market"] == "DOT/USD"
            and order["side"] == "sell"
            and order["status"] == "closed"
        ):
            avg_price = order["avgFillPrice"]
            size = order["filledSize"]
            total_usd += avg_price * size
            total_size += size

    if total_size > 0:
        avg_price = total_usd / total_size
    else:
        avg_price = 0

    logger.info(f"FTX - DOT Size: {total_size}")
    logger.info(f"FTX - DOT Avg Price: {avg_price}")
    logger.info(f"FTX - DOT Total USD: {total_usd}")

    return total_size, avg_price


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


@task(name="FTX DOT Balance Task")
def ftx_get_dot_balance():
    logger = get_run_logger()
    logger.info("FTX - Getting DOT Balance")

    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    balances = ftx.get_balances()
    dot_balance = 0
    for balance in balances:
        if balance["coin"] == "DOT":
            dot_balance = balance["total"]

    logger.info(f"FTX - DOT Balance: {dot_balance}")

    return dot_balance


@task
def ftx_get_invested_amount():
    logger = get_run_logger()
    logger.info("FTX - Getting invested amount")

    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    deposits = ftx.get_deposit_history()

    total_invested = 0
    for deposit in deposits:
        print(deposit)

    return total_invested

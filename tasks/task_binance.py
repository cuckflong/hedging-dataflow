from binance.client import Client
from prefect import get_run_logger, task
from prefect.blocks.system import Secret


@task(name="Binance DOT Cost Task")
def binance_get_dot_cost():
    logger = get_run_logger()
    logger.info("Binance - Getting DOT Cost")

    api_key = Secret.load("binance-api-key").get()
    api_secret = Secret.load("binance-api-secret").get()

    client = Client(api_key=api_key, api_secret=api_secret)
    orders = client.get_all_orders(symbol="DOTBUSD")

    total_usd: float = 0
    total_size: float = 0

    for order in orders:
        if order["status"].lower() == "filled" and order["side"].lower() == "buy":
            total_usd += float(order["cummulativeQuoteQty"])
            total_size += float(order["executedQty"])

    avg_cost = total_usd / total_size

    logger.info(f"Binance - DOT Size: {total_size}")
    logger.info(f"Binance - DOT Avg Price: {avg_cost}")
    logger.info(f"Binance - DOT Cost USD: {total_usd}")

    return total_size, avg_cost

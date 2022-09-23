from prefect import flow, get_run_logger
from prefect.blocks.system import Secret, String

from common.ftx_client import FtxClient


@flow(name="FTX DOT Cost Flow")
def ftx_dot_cost_flow():
    logger = get_run_logger()
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
    avg_cost = round(total_cost / total_size, 5)
    logger.info(f"FTX - Total cost (USD): {total_cost}")
    logger.info(f"FTX - Total size (DOT): {total_size}")
    logger.info(f"FTX - Average cost (USD): {avg_cost}")
    return total_cost, total_size, avg_cost


if __name__ == "__main__":
    ftx_dot_cost_flow()

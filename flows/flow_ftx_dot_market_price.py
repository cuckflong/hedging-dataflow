from prefect import flow, get_run_logger
from prefect.blocks.system import Secret, String

from common.ftx_client import FtxClient


@flow(name="FTX DOT Market Price Flow")
def ftx_dot_market_price_flow():
    logger = get_run_logger()
    api_key = Secret.load("ftx-api-key").get()
    api_secret = Secret.load("ftx-api-secret").get()
    subaccount_name = String.load("ftx-account").value
    ftx = FtxClient(
        api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name
    )
    dot_market = ftx.get_market("DOT/USD")
    market_price: float = dot_market["price"]
    logger.info(f"FTX - DOT Market price (USD): {market_price}")
    return market_price


if __name__ == "__main__":
    ftx_dot_market_price_flow()

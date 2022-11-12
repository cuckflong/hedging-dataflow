from prefect import flow, get_run_logger

from tasks.task_binance import binance_get_dot_cost, binance_get_dot_price


@flow(name="Collect Binance raw data")
def collect_binance_raw_data_flow():
    logger = get_run_logger()
    logger.info("Collecting Binance raw data")

    binance_cost_size, binance_avg_price = binance_get_dot_cost()

    binance_dot_market_price = binance_get_dot_price()

    return binance_cost_size, binance_avg_price, binance_dot_market_price

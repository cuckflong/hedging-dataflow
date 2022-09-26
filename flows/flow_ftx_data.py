from prefect import flow, get_run_logger

from tasks.task_ftx import (
    ftx_get_dot_cost,
    ftx_get_dot_market_price,
    ftx_get_dot_withdrawn,
)


@flow(name="Collect FTX raw data")
def collect_ftx_raw_data_flow():
    logger = get_run_logger()
    logger.info("Collecting FTX raw data")

    ftx_total_size, ftx_avg_cost = ftx_get_dot_cost()
    ftx_withdrawal_amount = ftx_get_dot_withdrawn()
    dot_market_price = ftx_get_dot_market_price()
    return ftx_total_size, ftx_avg_cost, ftx_withdrawal_amount, dot_market_price

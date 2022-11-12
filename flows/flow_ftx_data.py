from prefect import flow, get_run_logger

from tasks.task_ftx import ftx_get_dot_balance, ftx_get_dot_cost, ftx_get_dot_settlement


@flow(name="Collect FTX raw data")
def collect_ftx_raw_data_flow():
    logger = get_run_logger()
    logger.info("Collecting FTX raw data")

    (
        ftx_cost_size,
        ftx_cost_avg_price,
    ) = ftx_get_dot_cost()

    (
        ftx_settled_size,
        ftx_settled_avg_price,
    ) = ftx_get_dot_settlement()

    ftx_dot_balance = ftx_get_dot_balance()

    return (
        ftx_dot_balance,
        ftx_cost_size,
        ftx_cost_avg_price,
        ftx_settled_size,
        ftx_settled_avg_price,
    )

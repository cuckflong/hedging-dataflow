from prefect import flow, get_run_logger
from prefect.blocks.system import String

from tasks.task_pps import pps_get_all_data, pps_token_refresh


@flow(name="Collect PPS raw data")
def collect_pps_raw_data_flow():
    logger = get_run_logger()
    logger.info("Collecting PPS raw data")

    pps_get_all_data()

    pps_acct_balance = float(String.load("pps-acct-balance").value)

    pps_open_margin = float(String.load("pps-open-margin").value)
    pps_open_dot_size = float(String.load("pps-open-dot-size").value)
    pps_open_dot_avg_price = float(String.load("pps-open-dot-avg-price").value)
    pps_open_swap = float(String.load("pps-open-swap").value)

    pps_closed_swap = float(String.load("pps-closed-swap").value)
    pps_realized_pnl = float(String.load("pps-realized-pnl").value)

    return (
        pps_acct_balance,
        pps_open_margin,
        pps_open_dot_size,
        pps_open_dot_avg_price,
        pps_open_swap,
        pps_closed_swap,
        pps_realized_pnl,
    )


@flow(name="Refresh PPS token")
def pps_token_refresh_flow():
    logger = get_run_logger()
    logger.info("Refreshing PPS token")
    pps_token_refresh()

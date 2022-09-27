from prefect import flow, get_run_logger
from prefect.blocks.system import String

from tasks.task_pps import pps_get_all_data, pps_token_refresh


@flow(name="Collect PPS raw data")
def collect_pps_raw_data_flow():
    logger = get_run_logger()
    logger.info("Collecting PPS raw data")

    pps_get_all_data()

    pps_acct_balance = float(String.load("pps-last-acct-balance").value)
    pps_total_dot_size = float(String.load("pps-last-total-dot-size").value)
    pps_total_swap = float(String.load("pps-last-total-swap").value)
    pps_avg_entry_price = float(String.load("pps-last-avg-entry-price").value)
    return pps_acct_balance, pps_total_dot_size, pps_total_swap, pps_avg_entry_price


@flow(name="Refresh PPS token")
def pps_token_refresh_flow():
    logger = get_run_logger()
    logger.info("Refreshing PPS token")
    pps_token_refresh()

from prefect import flow, get_run_logger
from prefect.blocks.system import String

from tasks.task_dot import dot_get_balance, dot_get_rewards, dot_get_staked_balance


@flow(name="Collect DOT raw data")
def collect_dot_raw_data_flow():
    logger = get_run_logger()
    logger.info("Collecting DOT raw data")

    dot_address = String.load("dot-address").value

    dot_total_balance = dot_get_balance(dot_address)
    dot_staked_balance = dot_get_staked_balance(dot_address)
    dot_total_rewards = dot_get_rewards(dot_address)

    return dot_total_balance, dot_staked_balance, dot_total_rewards

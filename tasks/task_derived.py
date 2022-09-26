from prefect import get_run_logger, task

from common.utils import calc_net_profit


@task
def calc_pps_position_pnl(
    dot_market_price: float, pps_avg_entry_price: float, pps_total_dot_size: float
) -> float:
    logger = get_run_logger()
    logger.info("Calculating PPS position PNL")

    pps_position_pnl = (
        calc_net_profit(dot_market_price, pps_avg_entry_price, pps_total_dot_size) * -1
    )

    return pps_position_pnl


@task
def calc_ftx_position_pnl(
    dot_market_price: float, ftx_avg_cost: float, ftx_total_size: float
) -> float:
    logger = get_run_logger()
    logger.info("Calculating FTX position PNL")

    ftx_position_pnl = calc_net_profit(dot_market_price, ftx_avg_cost, ftx_total_size)

    return ftx_position_pnl


# without factoring in DOT withdrawn from Ledger, will be added in future
@task
def calc_dot_fees(dot_balance: float, ftx_total_size: float, dot_rewards: float):
    logger = get_run_logger()
    logger.info("Calculating DOT fees")

    dot_fees = ftx_total_size - dot_balance + dot_rewards

    return dot_fees


@task
def calc_pps_liq_value(
    pps_avg_entry_price: float, pps_total_dot_size: float, pps_position_pnl: float
) -> float:
    logger = get_run_logger()
    logger.info("Calculating PPS liquid value")

    position_entry_value = pps_avg_entry_price * pps_total_dot_size

    unrealised_pnl = -1 * pps_position_pnl

    pps_liq_value = position_entry_value + unrealised_pnl

    return pps_liq_value


@task
def calc_dot_liq_value(dot_market_price: float, dot_total_balance: float) -> float:
    logger = get_run_logger()
    logger.info("Calculating DOT liquid value")

    dot_liq_value = dot_market_price * dot_total_balance

    return dot_liq_value


@task
def calc_dot_net_position(pps_total_dot_size: float, dot_total_balance: float) -> float:
    logger = get_run_logger()
    logger.info("Calculating DOT net position")

    dot_net_position = dot_total_balance - pps_total_dot_size

    return dot_net_position

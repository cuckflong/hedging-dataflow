import time

from prefect import flow, get_run_logger

from flows.flow_dot_data import collect_dot_raw_data_flow
from flows.flow_ftx_data import collect_ftx_raw_data_flow
from flows.flow_pps_data import collect_pps_raw_data_flow
from tasks.task_db import (
    create_derived_data_table,
    create_raw_data_table,
    get_last_derived_value,
    write_derived_data_to_db,
    write_raw_data_to_db,
)


@flow(name="Collect all data")
def collect_all_data_flow(dry_run: bool = False):
    logger = get_run_logger()
    logger.info("Collecting raw data")

    create_raw_data_table()
    create_derived_data_table()

    unix_time = int(time.time())

    (
        ftx_dot_balance,
        ftx_cost_size,
        ftx_cost_avg_price,
        ftx_settled_size,
        ftx_settled_avg_price,
        dot_market_price,
    ) = collect_ftx_raw_data_flow()

    (
        dot_total_balance,
        dot_staked_balance,
        dot_total_rewards,
    ) = collect_dot_raw_data_flow()

    (
        pps_acct_balance,
        pps_open_margin,
        pps_open_dot_size,
        pps_open_dot_avg_price,
        pps_open_swap,
        pps_closed_margin,
        pps_closed_dot_size,
        pps_closed_dot_avg_price,
        pps_closed_swap,
    ) = collect_pps_raw_data_flow()

    logger.info("Collecting raw data complete")

    logger.info("Calculating derived data")

    pps_open_pnl = (pps_open_dot_avg_price - dot_market_price) * abs(pps_open_dot_size)
    pps_closed_pnl = (pps_closed_dot_avg_price - dot_market_price) * abs(
        pps_closed_dot_size
    )
    pps_open_liquid_value = pps_open_margin + pps_open_pnl + pps_open_swap
    pps_closed_liquid_value = pps_closed_margin + pps_closed_pnl + pps_closed_swap
    prev_closed_liq_value = get_last_derived_value("pps_closed_liquid_value")

    if prev_closed_liq_value is None:
        pps_closed_diff = 0
    else:
        pps_closed_diff = pps_closed_liquid_value - prev_closed_liq_value

    pps_total_swap = pps_open_swap + pps_closed_swap

    dot_liquid_value = (
        dot_total_balance * dot_market_price + ftx_dot_balance * dot_market_price
    )

    total_liquid_value = pps_open_liquid_value + dot_liquid_value

    prev_liquid_value = get_last_derived_value("total_liquid_value")

    if prev_liquid_value is None:
        liquid_value_diff = 0
    else:
        liquid_value_diff = total_liquid_value - prev_liquid_value

    total_cost = ftx_cost_size * ftx_cost_avg_price + pps_open_margin

    prev_cost = get_last_derived_value("total_cost")

    if prev_cost is None:
        cost_diff = 0
    else:
        cost_diff = total_cost - prev_cost

    total_settled = ftx_settled_size * ftx_settled_avg_price

    prev_settled = get_last_derived_value("total_settled")

    if prev_settled is None:
        settled_diff = 0
    else:
        settled_diff = total_settled - prev_settled

    staked_ratio = dot_staked_balance / (dot_total_balance + ftx_dot_balance) * 100

    margin_ratio = ((pps_acct_balance + pps_open_pnl) / pps_open_margin) * 100

    dot_net_position = dot_total_balance + ftx_dot_balance + pps_open_dot_size

    dot_fees = (
        ftx_cost_size
        - (ftx_settled_size + ftx_dot_balance + dot_total_balance)
        + dot_total_rewards
    )

    pnl = liquid_value_diff - cost_diff + pps_closed_diff + settled_diff

    logger.info("Calculating derived data complete")

    logger.info(f"Raw - FTX DOT balance: {ftx_dot_balance}")
    logger.info(f"Raw - FTX cost size: {ftx_cost_size}")
    logger.info(f"Raw - FTX cost avg price: {ftx_cost_avg_price}")
    logger.info(f"Raw - FTX settled size: {ftx_settled_size}")
    logger.info(f"Raw - FTX settled avg price: {ftx_settled_avg_price}")
    logger.info(f"Raw - DOT market price: {dot_market_price}")
    logger.info(f"Raw - DOT total balance: {dot_total_balance}")
    logger.info(f"Raw - DOT staked balance: {dot_staked_balance}")
    logger.info(f"Raw - DOT total rewards: {dot_total_rewards}")
    logger.info(f"Raw - PPS account balance: {pps_acct_balance}")
    logger.info(f"Raw - PPS open margin: {pps_open_margin}")
    logger.info(f"Raw - PPS open DOT size: {pps_open_dot_size}")
    logger.info(f"Raw - PPS open DOT avg price: {pps_open_dot_avg_price}")
    logger.info(f"Raw - PPS open swap: {pps_open_swap}")
    logger.info(f"Raw - PPS closed margin: {pps_closed_margin}")
    logger.info(f"Raw - PPS closed DOT size: {pps_closed_dot_size}")
    logger.info(f"Raw - PPS closed DOT avg price: {pps_closed_dot_avg_price}")
    logger.info(f"Raw - PPS closed swap: {pps_closed_swap}")
    logger.info(f"Derived - PPS open PnL: {pps_open_pnl}")
    logger.info(f"Derived - PPS closed PnL: {pps_closed_pnl}")
    logger.info(f"Derived - PPS open liquid value: {pps_open_liquid_value}")
    logger.info(f"Derived - PPS closed liquid value: {pps_closed_liquid_value}")
    logger.info(f"Derived - PPS total swap: {pps_total_swap}")
    logger.info(f"Derived - DOT liquid value: {dot_liquid_value}")
    logger.info(f"Derived - Total liquid value: {total_liquid_value}")
    logger.info(f"Derived - Total cost: {total_cost}")
    logger.info(f"Derived - Total settled: {total_settled}")
    logger.info(f"Derived - Staked ratio: {staked_ratio}")
    logger.info(f"Derived - Margin ratio: {margin_ratio}")
    logger.info(f"Derived - DOT net position: {dot_net_position}")
    logger.info(f"Derived - DOT fees: {dot_fees}")
    logger.info(f"Derived - PnL: {pnl}")

    if dry_run:
        return

    write_raw_data_to_db(
        unix_time,
        ftx_dot_balance,
        ftx_cost_size,
        ftx_cost_avg_price,
        ftx_settled_size,
        ftx_settled_avg_price,
        dot_market_price,
        dot_total_balance,
        dot_staked_balance,
        dot_total_rewards,
        pps_acct_balance,
        pps_open_margin,
        pps_open_dot_size,
        pps_open_dot_avg_price,
        pps_open_swap,
        pps_closed_margin,
        pps_closed_dot_size,
        pps_closed_dot_avg_price,
        pps_closed_swap,
    )

    write_derived_data_to_db(
        unix_time,
        pps_open_pnl,
        pps_closed_pnl,
        pps_open_liquid_value,
        pps_closed_liquid_value,
        pps_total_swap,
        dot_liquid_value,
        total_liquid_value,
        total_cost,
        total_settled,
        staked_ratio,
        margin_ratio,
        dot_net_position,
        dot_fees,
        pnl,
    )


if __name__ == "__main__":
    collect_all_data_flow(dry_run=True)

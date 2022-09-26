import time

from prefect import flow, get_run_logger

from flows.flow_dot_data import collect_dot_raw_data_flow
from flows.flow_ftx_data import collect_ftx_raw_data_flow
from flows.flow_pps_data import collect_pps_raw_data_flow
from tasks.task_db import (
    create_derived_data_table,
    create_raw_data_table,
    get_last_total_liq_value,
    write_derived_data_to_db,
    write_raw_data_to_db,
)
from tasks.task_derived import (
    calc_dot_fees,
    calc_dot_liq_value,
    calc_dot_net_position,
    calc_ftx_position_pnl,
    calc_pps_liq_value,
    calc_pps_position_pnl,
)


@flow(name="Collect all data")
def collect_all_data_flow(dry_run: bool = False):
    logger = get_run_logger()
    logger.info("Collecting raw data")

    create_raw_data_table()
    create_derived_data_table()

    unix_time = int(time.time())

    (
        ftx_total_size,
        ftx_avg_cost,
        ftx_withdrawal_amount,
        dot_market_price,
    ) = collect_ftx_raw_data_flow()

    (
        dot_total_balance,
        dot_staked_balance,
        dot_total_rewards,
    ) = collect_dot_raw_data_flow()

    (
        pps_total_dot_size,
        pps_total_swap,
        pps_avg_entry_price,
    ) = collect_pps_raw_data_flow()

    logger.info(f"FTX total size: {ftx_total_size}")
    logger.info(f"FTX avg cost: {ftx_avg_cost}")
    logger.info(f"FTX withdrawal amount: {ftx_withdrawal_amount}")
    logger.info(f"DOT market price: {dot_market_price}")
    logger.info(f"DOT total balance: {dot_total_balance}")
    logger.info(f"DOT staked balance: {dot_staked_balance}")
    logger.info(f"DOT total rewards: {dot_total_rewards}")
    logger.info(f"PPS total DOT size: {pps_total_dot_size}")
    logger.info(f"PPS total swap: {pps_total_swap}")
    logger.info(f"PPS avg entry price: {pps_avg_entry_price}")

    logger.info("Collecting raw data complete")

    logger.info("Calculating derived data")

    pps_position_pnl = calc_pps_position_pnl(
        dot_market_price, pps_avg_entry_price, pps_total_dot_size
    )

    ftx_position_pnl = calc_ftx_position_pnl(
        dot_market_price, ftx_avg_cost, ftx_total_size
    )

    dot_fees = calc_dot_fees(dot_total_balance, ftx_total_size, dot_total_rewards)

    pps_liq_value = calc_pps_liq_value(
        pps_avg_entry_price, pps_total_dot_size, pps_position_pnl
    )

    dot_liq_value = calc_dot_liq_value(dot_market_price, dot_total_balance)

    total_liq_value = pps_liq_value + dot_liq_value

    dot_net_position = calc_dot_net_position(pps_total_dot_size, dot_total_balance)

    usd_net_position = dot_net_position * dot_market_price

    prev_total_liq_value = get_last_total_liq_value()

    if prev_total_liq_value == 0:
        pnl = 0
    else:
        pnl = total_liq_value - prev_total_liq_value

    total_interest = dot_total_rewards + pps_total_swap

    logger.info(f"PPS position PnL: {pps_position_pnl}")
    logger.info(f"FTX position PnL: {ftx_position_pnl}")
    logger.info(f"DOT fees: {dot_fees}")
    logger.info(f"PPS liquidation value: {pps_liq_value}")
    logger.info(f"DOT liquidation value: {dot_liq_value}")
    logger.info(f"Total liquidation value: {total_liq_value}")
    logger.info(f"DOT net position: {dot_net_position}")
    logger.info(f"USD net position: {usd_net_position}")
    logger.info(f"Total PnL: {pnl}")
    logger.info(f"Total interests: {total_interest}")

    logger.info("Calculating derived data complete")

    if dry_run:
        return

    write_raw_data_to_db(
        unix_time,
        ftx_total_size,
        ftx_avg_cost,
        ftx_withdrawal_amount,
        dot_market_price,
        dot_total_balance,
        dot_staked_balance,
        dot_total_rewards,
        pps_total_dot_size,
        pps_total_swap,
        pps_avg_entry_price,
    )

    write_derived_data_to_db(
        unix_time,
        pps_position_pnl,
        ftx_position_pnl,
        pps_liq_value,
        dot_liq_value,
        total_liq_value,
        dot_net_position,
        usd_net_position,
        dot_fees,
        total_interest,
        pnl,
    )


if __name__ == "__main__":
    collect_all_data_flow(dry_run=True)

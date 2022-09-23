import time

import psycopg2
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret, String

from .flow_ctrader_agg_pos import ctrader_get_positions_data
from .flow_ftx_dot_cost import ftx_dot_cost_flow
from .flow_ftx_dot_market_price import ftx_dot_market_price_flow
from .flow_ledger_dot_balance import dot_balance_flow


@task
def create_table():
    logger = get_run_logger()

    logger.info("Creating table if not exists")

    database = Secret.load("prefect-psql-database").get()
    host = Secret.load("prefect-psql-host").get()
    user = Secret.load("prefect-psql-user").get()
    password = Secret.load("prefect-psql-password").get()
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hedge_data (
            id SERIAL PRIMARY KEY,
            unix_time BIGINT NOT NULL,
            ftx_total_cost DECIMAL,
            ftx_total_size DECIMAL,
            ftx_avg_cost DECIMAL,
            ftx_dot_market_price DECIMAL,
            ledger_dot_total_balance DECIMAL,
            ledger_dot_staked_balance DECIMAL,
            ctrader_total_dot_size DECIMAL,
            ctrader_total_swap DECIMAL,
            ctrader_entry_position_size DECIMAL,
            ctrader_avg_entry_price DECIMAL,
            ctrader_pnl DECIMAL,
            ctrader_net_position_size DECIMAL,
            ftx_pnl DECIMAL,
            ftx_net_position_size DECIMAL,
            dot_fee_unit DECIMAL,
            dot_fee_usd DECIMAL
        );
    """
    )
    conn.commit()
    cur.close()
    conn.close()


@task
def write_data_to_db(
    ftx_total_cost,
    ftx_total_size,
    ftx_avg_cost,
    dot_market_price,
    dot_total_balance,
    dot_staked_balance,
    ctrader_total_dot_size,
    ctrader_total_swap,
    ctrader_entry_position_size,
    ctrader_avg_entry_price,
    ctrader_pnl,
    ctrader_net_position_size,
    ftx_pnl,
    ftx_net_position_size,
    dot_fee_unit,
    dot_fee_usd,
):
    logger = get_run_logger()

    logger.info("Writing data to db")

    database = Secret.load("prefect-psql-database").get()
    host = Secret.load("prefect-psql-host").get()
    user = Secret.load("prefect-psql-user").get()
    password = Secret.load("prefect-psql-password").get()
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    unix_time = int(time.time())

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO hedge_data (
            unix_time,
            ftx_total_cost,
            ftx_total_size,
            ftx_avg_cost,
            ftx_dot_market_price,
            ledger_dot_total_balance,
            ledger_dot_staked_balance,
            ctrader_total_dot_size,
            ctrader_total_swap,
            ctrader_entry_position_size,
            ctrader_avg_entry_price,
            ctrader_pnl,
            ctrader_net_position_size,
            ftx_pnl,
            ftx_net_position_size,
            dot_fee_unit,
            dot_fee_usd
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
            unix_time,
            ftx_total_cost,
            ftx_total_size,
            ftx_avg_cost,
            dot_market_price,
            dot_total_balance,
            dot_staked_balance,
            ctrader_total_dot_size,
            ctrader_total_swap,
            ctrader_entry_position_size,
            ctrader_avg_entry_price,
            ctrader_pnl,
            ctrader_net_position_size,
            ftx_pnl,
            ftx_net_position_size,
            dot_fee_unit,
            dot_fee_usd,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


@task
def calc_position_diff(market_price: float, total_size: float, avg_cost: float):
    net_profit = (market_price - avg_cost) * total_size
    return net_profit


@flow(name="Hedge Data Flow")
def hedge_data_flow():
    logger = get_run_logger()

    dot_address = String.load("dot-address").value

    ftx_total_cost, ftx_total_size, ftx_avg_cost = ftx_dot_cost_flow()
    dot_market_price = ftx_dot_market_price_flow()
    dot_total_balance, dot_staked_balance = dot_balance_flow(dot_address)
    ctrader_get_positions_data()

    ctrader_total_dot_size = float(String.load("ctrader-last-total-dot-size").value)
    ctrader_total_swap = float(String.load("ctrader-last-total-swap").value)
    ctrader_entry_position_size = float(
        String.load("ctrader-last-entry-position-size").value
    )
    ctrader_avg_entry_price = float(String.load("ctrader-last-avg-entry-price").value)

    ctrader_pnl = round(
        calc_position_diff(
            dot_market_price, ctrader_total_dot_size, ctrader_avg_entry_price
        )
        * -1,
        5,
    )

    ctrader_net_position_size = ctrader_entry_position_size + ctrader_pnl

    ftx_pnl = round(
        calc_position_diff(dot_market_price, ftx_total_size, ftx_avg_cost), 5
    )

    ftx_net_position_size = ftx_total_cost + ftx_pnl

    dot_fee_unit = round(ftx_total_size - dot_total_balance, 5)
    dot_fee_usd = round(dot_fee_unit * dot_market_price, 5)

    logger.info(f"FTX - Total cost (USD): {ftx_total_cost}")
    logger.info(f"FTX - Total size (DOT): {ftx_total_size}")
    logger.info(f"FTX - Avg cost (USD): {ftx_avg_cost}")
    logger.info(f"FTX - Market price (USD): {dot_market_price}")
    logger.info(f"Ledger - Total balance (DOT): {dot_total_balance}")
    logger.info(f"Ledger - Staked balance (DOT): {dot_staked_balance}")
    logger.info(f"CTrader - Total DOT size: {ctrader_total_dot_size}")
    logger.info(f"CTrader - Total swap: {ctrader_total_swap}")
    logger.info(f"CTrader - Entry position size: {ctrader_entry_position_size}")
    logger.info(f"CTrader - Average entry price: {ctrader_avg_entry_price}")
    logger.info(f"CTrader - PnL: {ctrader_pnl}")
    logger.info(f"FTX - PnL: {ftx_pnl}")
    logger.info(f"CTrader - Net position size: {ctrader_net_position_size}")
    logger.info(f"FTX - Net position size: {ftx_net_position_size}")
    logger.info(f"DOT fee unit: {dot_fee_unit}")
    logger.info(f"DOT fee USD: {dot_fee_usd}")

    create_table()
    write_data_to_db(
        ftx_total_cost,
        ftx_total_size,
        ftx_avg_cost,
        dot_market_price,
        dot_total_balance,
        dot_staked_balance,
        ctrader_total_dot_size,
        ctrader_total_swap,
        ctrader_entry_position_size,
        ctrader_avg_entry_price,
        ctrader_pnl,
        ctrader_net_position_size,
        ftx_pnl,
        ftx_net_position_size,
        dot_fee_unit,
        dot_fee_usd,
    )


if __name__ == "__main__":
    hedge_data_flow()

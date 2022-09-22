import time
from prefect import flow, task, get_run_logger
from prefect.blocks.system import String, Secret

import psycopg2

from .flow_ftx_dot_cost import ftx_dot_cost_flow
from .flow_ftx_dot_market_price import ftx_dot_market_price_flow
from .flow_ledger_dot_balance import dot_balance_flow
from .flow_ctrader_agg_pos import ctrader_get_positions_data


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
            ftx_dot_market_price DECIMAL,
            ledger_dot_total_balance DECIMAL,
            ledger_dot_staked_balance DECIMAL,
            ctrader_total_dot_size DECIMAL,
            ctrader_total_swap DECIMAL,
            ctrader_total_position_size DECIMAL,
            ctrader_avg_entry_price DECIMAL
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
    dot_market_price,
    dot_total_balance,
    dot_staked_balance,
    total_dot_size,
    total_swap,
    total_position_size,
    avg_entry_price,
):
    logger = get_run_logger()

    logger.info("Writing data to db")

    database = Secret.load("prefect-psql-database").get()
    user = Secret.load("prefect-psql-user").get()
    password = Secret.load("prefect-psql-password").get()
    conn = psycopg2.connect(
        host="127.0.0.1",
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
            ftx_dot_market_price,
            ledger_dot_total_balance,
            ledger_dot_staked_balance,
            ctrader_total_dot_size,
            ctrader_total_swap,
            ctrader_total_position_size,
            ctrader_avg_entry_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
            unix_time,
            ftx_total_cost,
            ftx_total_size,
            dot_market_price,
            dot_total_balance,
            dot_staked_balance,
            total_dot_size,
            total_swap,
            total_position_size,
            avg_entry_price,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


@flow(name="Hedge Data Flow")
def hedge_data_flow():
    logger = get_run_logger()

    dot_address = String.load("dot-address").value

    ftx_total_cost, ftx_total_size = ftx_dot_cost_flow()
    dot_market_price = ftx_dot_market_price_flow()
    dot_total_balance, dot_staked_balance = dot_balance_flow(dot_address)
    ctrader_get_positions_data()

    total_dot_size = String.load("ctrader-last-total-dot-size").value
    total_swap = String.load("ctrader-last-total-swap").value
    total_position_size = String.load("ctrader-last-total-position-size").value
    avg_entry_price = String.load("ctrader-last-avg-entry-price").value

    logger.info(f"FTX - Total cost (USD): {ftx_total_cost}")
    logger.info(f"FTX - Total size (DOT): {ftx_total_size}")
    logger.info(f"FTX - Market price (USD): {dot_market_price}")
    logger.info(f"Ledger - Total balance (DOT): {dot_total_balance}")
    logger.info(f"Ledger - Staked balance (DOT): {dot_staked_balance}")
    logger.info(f"CTrader - Total DOT size: {total_dot_size}")
    logger.info(f"CTrader - Total swap: {total_swap}")
    logger.info(f"CTrader - Total position size: {total_position_size}")
    logger.info(f"CTrader - Average entry price: {avg_entry_price}")

    create_table()
    write_data_to_db(
        ftx_total_cost,
        ftx_total_size,
        dot_market_price,
        dot_total_balance,
        dot_staked_balance,
        total_dot_size,
        total_swap,
        total_position_size,
        avg_entry_price,
    )


if __name__ == "__main__":
    hedge_data_flow()

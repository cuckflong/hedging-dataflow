import psycopg2
from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from psycopg2 import sql


@task
def drop_table(table_name: str):
    logger = get_run_logger()

    logger.info("Dropping table if exists")

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
        sql.SQL("DROP TABLE IF EXISTS {table_name}").format(
            table_name=sql.Identifier(table_name)
        )
    )

    conn.commit()
    cur.close()
    conn.close()


@task
def create_raw_data_table():
    logger = get_run_logger()

    logger.info("Creating raw data table if not exists")

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
        CREATE TABLE IF NOT EXISTS hedge_data_raw (
            id SERIAL PRIMARY KEY,
            unix_time BIGINT NOT NULL,
            ftx_dot_balance DOUBLE PRECISION,
            ftx_cost_size DOUBLE PRECISION,
            ftx_cost_avg_price DOUBLE PRECISION,
            ftx_settled_size DOUBLE PRECISION,
            ftx_settled_avg_price DOUBLE PRECISION,
            dot_market_price DOUBLE PRECISION,
            dot_total_balance DOUBLE PRECISION,
            dot_staked_balance DOUBLE PRECISION,
            dot_total_rewards DOUBLE PRECISION,
            pps_acct_balance DOUBLE PRECISION,
            pps_open_margin DOUBLE PRECISION,
            pps_open_dot_size DOUBLE PRECISION,
            pps_open_dot_avg_price DOUBLE PRECISION,
            pps_open_swap DOUBLE PRECISION,
            pps_closed_margin DOUBLE PRECISION,
            pps_closed_dot_size DOUBLE PRECISION,
            pps_closed_dot_avg_price DOUBLE PRECISION,
            pps_closed_swap DOUBLE PRECISION
        );
    """
    )
    conn.commit()
    cur.close()
    conn.close()
    return


@task
def create_derived_data_table():
    logger = get_run_logger()

    logger.info("Creating raw data table if not exists")

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
        CREATE TABLE IF NOT EXISTS hedge_data_derived (
            id SERIAL PRIMARY KEY,
            unix_time BIGINT NOT NULL,
            pps_open_pnl DOUBLE PRECISION,
            pps_closed_pnl DOUBLE PRECISION,
            pps_open_liquid_value DOUBLE PRECISION,
            pps_closed_liquid_value DOUBLE PRECISION,
            pps_total_swap DOUBLE PRECISION,
            dot_liquid_value DOUBLE PRECISION,
            total_liquid_value DOUBLE PRECISION,
            total_cost DOUBLE PRECISION,
            total_settled DOUBLE PRECISION,
            staked_ratio DOUBLE PRECISION,
            margin_ratio DOUBLE PRECISION,
            dot_net_position DOUBLE PRECISION,
            dot_fees DOUBLE PRECISION,
            pnl DOUBLE PRECISION
        );
    """
    )
    conn.commit()
    cur.close()
    conn.close()
    return


@task
def write_raw_data_to_db(
    unix_time: int,
    ftx_dot_balance: float,
    ftx_cost_size: float,
    ftx_cost_avg_price: float,
    ftx_settled_size: float,
    ftx_settled_avg_price: float,
    dot_market_price: float,
    dot_total_balance: float,
    dot_staked_balance: float,
    dot_total_rewards: float,
    pps_acct_balance: float,
    pps_open_margin: float,
    pps_open_dot_size: float,
    pps_open_dot_avg_price: float,
    pps_open_swap: float,
    pps_closed_margin: float,
    pps_closed_dot_size: float,
    pps_closed_dot_avg_price: float,
    pps_closed_swap: float,
):
    logger = get_run_logger()

    logger.info("Writing raw data to db")

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
        INSERT INTO hedge_data_raw (
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
            pps_closed_swap
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
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
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    return


@task
def write_derived_data_to_db(
    unix_time: int,
    pps_open_pnl: float,
    pps_closed_pnl: float,
    pps_open_liquid_value: float,
    pps_closed_liquid_value: float,
    pps_total_swap: float,
    dot_liquid_value: float,
    total_liquid_value: float,
    total_cost: float,
    total_settled: float,
    staked_ratio: float,
    margin_ratio: float,
    dot_net_position: float,
    dot_fees: float,
    pnl: float,
):
    logger = get_run_logger()

    logger.info("Writing raw data to db")

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
        INSERT INTO hedge_data_derived (
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
            pnl
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
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
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    return


@task
def get_last_derived_value(col_name: str) -> float:
    logger = get_run_logger()

    logger.info(f"Getting last {col_name} value")

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
        f"SELECT {col_name} FROM hedge_data_derived ORDER BY unix_time DESC LIMIT 1;"
    )
    results = cur.fetchall()
    if len(results) == 0:
        return None
    else:
        derived_value = float(results[0][0])

    cur.close()
    conn.close()
    return derived_value

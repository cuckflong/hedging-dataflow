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
            ftx_total_size DOUBLE PRECISION,
            ftx_avg_cost DOUBLE PRECISION,
            ftx_withdrawal_amount DOUBLE PRECISION,
            dot_market_price DOUBLE PRECISION,
            dot_total_balance DOUBLE PRECISION,
            dot_staked_balance DOUBLE PRECISION,
            dot_total_rewards DOUBLE PRECISION,
            pps_acct_balance DOUBLE PRECISION,
            pps_total_dot_size DOUBLE PRECISION,
            pps_total_swap DOUBLE PRECISION,
            pps_avg_entry_price DOUBLE PRECISION
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
            pps_position_pnl DOUBLE PRECISION,
            ftx_position_pnl DOUBLE PRECISION,
            pps_liq_value DOUBLE PRECISION,
            dot_liq_value DOUBLE PRECISION,
            total_liq_value DOUBLE PRECISION,
            dot_net_position DOUBLE PRECISION,
            usd_net_position DOUBLE PRECISION,
            dot_fees DOUBLE PRECISION,
            total_interest DOUBLE PRECISION,
            interest_pnl DOUBLE PRECISION,
            position_pnl DOUBLE PRECISION,
            total_pnl DOUBLE PRECISION
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
    ftx_total_size: float,
    ftx_avg_cost: float,
    ftx_withdrawal_amount: float,
    dot_market_price: float,
    dot_total_balance: float,
    dot_staked_balance: float,
    dot_total_rewards: float,
    pps_acct_balance: float,
    pps_total_dot_size: float,
    pps_total_swap: float,
    pps_avg_entry_price: float,
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
            ftx_total_size,
            ftx_avg_cost,
            ftx_withdrawal_amount,
            dot_market_price,
            dot_total_balance,
            dot_staked_balance,
            dot_total_rewards,
            pps_acct_balance,
            pps_total_dot_size,
            pps_total_swap,
            pps_avg_entry_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
            unix_time,
            ftx_total_size,
            ftx_avg_cost,
            ftx_withdrawal_amount,
            dot_market_price,
            dot_total_balance,
            dot_staked_balance,
            dot_total_rewards,
            pps_acct_balance,
            pps_total_dot_size,
            pps_total_swap,
            pps_avg_entry_price,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    return


@task
def write_derived_data_to_db(
    unix_time: int,
    pps_position_pnl: float,
    ftx_position_pnl: float,
    pps_liq_value: float,
    dot_liq_value: float,
    total_liq_value: float,
    dot_net_position: float,
    usd_net_position: float,
    dot_fees: float,
    total_interest: float,
    interest_pnl: float,
    position_pnl: float,
    total_pnl: float,
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
            pps_position_pnl,
            ftx_position_pnl,
            pps_liq_value,
            dot_liq_value,
            total_liq_value,
            dot_net_position,
            usd_net_position,
            dot_fees,
            total_interest,
            interest_pnl,
            position_pnl,
            total_pnl
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
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
            interest_pnl,
            position_pnl,
            total_pnl,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    return


@task
def get_last_total_liq_value() -> float:
    logger = get_run_logger()

    logger.info("Getting last total liq value")

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
        SELECT total_liq_value FROM hedge_data_derived ORDER BY unix_time DESC LIMIT 1;
    """
    )
    results = cur.fetchall()
    if len(results) == 0:
        last_total_liq_value = 0
    else:
        last_total_liq_value = float(results[0][0])

    cur.close()
    conn.close()
    return last_total_liq_value


@task
def get_last_total_interest_value() -> float:
    logger = get_run_logger()

    logger.info("Getting last total interest value")

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
        SELECT total_interest FROM hedge_data_derived ORDER BY unix_time DESC LIMIT 1;
        """
    )
    results = cur.fetchall()
    if len(results) == 0:
        last_total_interest = 0
    else:
        last_total_interest = float(results[0][0])

    cur.close()
    conn.close()
    return last_total_interest

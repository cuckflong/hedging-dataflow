import psycopg2
from prefect import get_run_logger, task
from prefect.blocks.system import Secret


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
    cur.execute("""DROP TABLE IF EXISTS %S;""", (table_name))

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
            ftx_total_size DECIMAL,
            ftx_avg_cost DECIMAL,
            ftx_withdrawal_amount DECIMAL,
            dot_market_price DECIMAL,
            dot_total_balance DECIMAL,
            dot_staked_balance DECIMAL,
            dot_total_rewards DECIMAL,
            pps_total_dot_size DECIMAL,
            pps_total_swap DECIMAL,
            pps_avg_entry_price DECIMAL
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
            pps_position_pnl DECIMAL,
            ftx_position_pnl DECIMAL,
            pps_liq_value DECIMAL,
            dot_liq_value DECIMAL,
            total_liq_value DECIMAL,
            dot_net_position DECIMAL,
            usd_net_position DECIMAL,
            dot_fees DECIMAL,
            total_interest DECIMAL,
            pnl DECIMAL
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
            pps_total_dot_size,
            pps_total_swap,
            pps_avg_entry_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
            pps_position_pnl,
            ftx_position_pnl,
            pps_liq_value,
            dot_liq_value,
            total_liq_value,
            dot_net_position,
            usd_net_position,
            dot_fees,
            total_interest,
            pnl
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
            pnl,
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

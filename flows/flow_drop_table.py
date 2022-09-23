from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

import psycopg2


@task
def drop_table():
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
        """
        DROP TABLE IF EXISTS hedge_data;
    """
    )
    conn.commit()
    cur.close()
    conn.close()


@flow
def drop_table_flow():
    drop_table()


def main():
    drop_table_flow()


if __name__ == "__main__":
    main()

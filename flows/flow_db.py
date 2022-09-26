from prefect import flow, get_run_logger

from tasks.task_db import drop_table


@flow(name="Drop table")
def drop_table_flow(table_name: str):
    logger = get_run_logger()
    logger.info(f"Dropping table {table_name}")

    if not table_name:
        logger.error("No table name provided")
        return

    drop_table(table_name)

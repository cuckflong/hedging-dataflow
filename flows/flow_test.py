from prefect import flow

from tasks.task_db import drop_table


@flow
def flow_test():
    drop_table("hedge_data_raw")


if __name__ == "__main__":
    flow_test()

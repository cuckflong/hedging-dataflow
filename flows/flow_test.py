from prefect import flow

from tasks.task_ftx import ftx_get_invested_amount
from tasks.task_pps import pps_get_account_balance


@flow
def flow_test():
    pps_get_account_balance()


if __name__ == "__main__":
    flow_test()

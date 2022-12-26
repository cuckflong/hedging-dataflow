from prefect import flow

from tasks.task_pps import pps_get_all_data


@flow
def flow_test():
    pps_get_all_data()


if __name__ == "__main__":
    flow_test()

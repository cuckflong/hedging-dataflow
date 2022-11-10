from prefect import flow

from flows.flow_binance_data import collect_binance_raw_data_flow


@flow
def flow_test():
    collect_binance_raw_data_flow()


if __name__ == "__main__":
    flow_test()

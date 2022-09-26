from prefect.deployments import Deployment
from prefect.filesystems import RemoteFileSystem

from flows.flow_db import drop_table_flow


def main():
    remote_file_system_block = RemoteFileSystem.load("storage-hedge-pnl")
    deployment = Deployment.build_from_flow(
        flow=drop_table_flow,
        name="Drop Table Deployment",
        work_queue_name="staking-pnl-env",
        storage=remote_file_system_block,
    )
    deployment.apply()


if __name__ == "__main__":
    main()

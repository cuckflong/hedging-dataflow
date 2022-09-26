from prefect.deployments import Deployment
from prefect.filesystems import RemoteFileSystem
from prefect.orion.schemas.schedules import CronSchedule

from flows.flow_all_data import collect_all_data_flow


def main():
    remote_file_system_block = RemoteFileSystem.load("storage-hedge-pnl")
    deployment = Deployment.build_from_flow(
        flow=collect_all_data_flow,
        name="Collect All Hedge Data Deployment",
        work_queue_name="staking-pnl-env",
        storage=remote_file_system_block,
        # every day at 00:00, 06:00, 12:00, 18:00 HKT
        schedule=(CronSchedule(cron="0 */6 * * *", timezone="Asia/Hong_Kong")),
    )
    deployment.apply()


if __name__ == "__main__":
    main()

from prefect.deployments import Deployment
from prefect.filesystems import RemoteFileSystem
from prefect.orion.schemas.schedules import CronSchedule

from flows.flow_ctrader_token_refresh import ctrader_token_refresh


def main():
    remote_file_system_block = RemoteFileSystem.load("storage-hedge-pnl")
    deployment = Deployment.build_from_flow(
        flow=ctrader_token_refresh,
        name="cTrader Weekly Token Refresh Deployment",
        work_queue_name="staking-pnl-env",
        storage=remote_file_system_block,
        # every week on Saturday at 23:50 HKT
        schedule=(CronSchedule(cron="50 23 * * 6", timezone="Asia/Hong_Kong")),
    )
    deployment.apply()


if __name__ == "__main__":
    main()

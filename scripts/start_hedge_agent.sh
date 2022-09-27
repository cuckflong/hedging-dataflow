#!/bin/sh

poetry run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
poetry run prefect agent start  --work-queue "staking-pnl-env"
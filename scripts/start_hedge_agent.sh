#!/bin/sh

poetry shell
poetry install

prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect agent start  --work-queue "staking-pnl-env"
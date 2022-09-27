#!/bin/sh

/home/prefect/.local/bin/poetry shell
/home/prefect/.local/bin/poetry install

prefect config set PREFECT_API_URL=https://prefect.cuckflong.io/api
prefect orion start
#!/bin/sh

/home/prefect/.local/bin/poetry run prefect config set PREFECT_API_URL=https://prefect.cuckflong.io/api
/home/prefect/.local/bin/poetry run prefect orion start
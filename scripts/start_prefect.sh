#!/bin/sh

poetry run prefect config set PREFECT_API_URL=https://prefect.cuckflong.io/api
poetry run prefect orion start
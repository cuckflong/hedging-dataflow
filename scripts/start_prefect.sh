#!/bin/sh

poetry shell
poetry install

prefect config set PREFECT_API_URL=https://prefect.cuckflong.io/api
prefect orion start
#!/bin/sh

/home/prefect/.local/bin/poetry shell
/home/prefect/.local/bin/poetry install

prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

for f in ./deployments/*.py
do
    deploy=$(echo $f | cut -d '/' -f 3 | cut -d '.' -f1)
    python -m deployments.$deploy
done
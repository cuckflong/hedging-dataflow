#!/bin/sh

$HOME/miniconda3/bin/activate prefect
prefect config set PREFECT_API_URL=https://prefect.cuckflong.io/api
prefect orion start
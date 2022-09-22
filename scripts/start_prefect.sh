#!/bin/sh

source $HOME/miniconda3/etc/profile.d/conda.sh
conda activate prefect

prefect config set PREFECT_API_URL=https://prefect.cuckflong.io/api
prefect orion start

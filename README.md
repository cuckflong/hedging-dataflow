# hedging-dataflow

## Requirements

```
curl -sSL https://install.python-poetry.org | python3 -

sudo apt update
sudo apt install libpq-dev
```

## Install

```
poetry install
```

## Use environment

```
poetry shell
```

## Start Prefect Server

```
bash ./scripts/start_prefect.sh
```

## Apply all deployments

```
bash ./scripts/deploy.sh
```

## Run agent

```
bash ./scripts/start_hedge_agent.sh
```

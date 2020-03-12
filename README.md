# GOB-Test

Contains the E2E-style tests for GOB

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
docker-compose up &
```

## Tests

```bash
docker-compose -f src/.jenkins/test/docker-compose.yml build
docker-compose -f src/.jenkins/test/docker-compose.yml run test
```

# Local

## Requirements

* python >= 3.6

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment

```bash
source venv/bin/activate
```

# Run

Start the service:

```bash
cd src
python -m gobtest 
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```

# Remarks

## Trigger tests

Tests are triggered by the GOB-Workflow module. See the GOB-Workflow README for more details

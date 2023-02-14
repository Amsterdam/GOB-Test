#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"


echo "Running mypy"
mypy gobtest

echo "\nRunning unit tests"
coverage run --source=gobtest -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds no potential reformat fixes"
black --check --diff gobtest

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobtest gobtest

echo "\nRunning Flake8 style checks"
flake8 gobtest

echo "\nChecks complete"

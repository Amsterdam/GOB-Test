#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Coverage 6: coverage run --data-file=/tmp/.coveragerc …
export COVERAGE_FILE="/tmp/.coverage"

# Uncomment files to pass through checks
FILES=(
#  "gobtest/data_consistency/handler.py"
#  "gobtest/data_consistency/data_consistency_test.py"
  "gobtest/data_consistency/__init__.py"
  "gobtest/config.py"
  "gobtest/__init__.py"
#  "gobtest/e2e/handler.py"
#  "gobtest/e2e/e2etest.py"
  "gobtest/e2e/__init__.py"
  "gobtest/e2e/expect/__init__.py"
#  "gobtest/__main__.py"
)

echo "Running mypy"
mypy "${FILES[@]}"

echo "Running unit tests"
coverage run --source=gobtest -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "Check if Black finds no potential reformat fixes"
black --check --diff "${FILES[@]}"

echo "Check for potential import sort"
isort --check --diff "${FILES[@]}"

echo "Running flake8 style checks"
flake8 "${FILES[@]}"

echo "Checks complete"

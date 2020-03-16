#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Clear any cached results
find . -name "*.pyc" -exec rm -f {} \;

echo "Running style checks"
flake8

echo "Running unit tests"
pytest tests/

echo "Running coverage tests"

pytest tests/ --cov=gobtest --cov-report html --cov-fail-under=100

#!/bin/bash

set -e

echo "Running type checks"
poetry run mypy --ignore-missing-imports \
--disallow-untyped-calls --disallow-untyped-defs \
--disallow-incomplete-defs \
transformations tests

echo "Running lint checks"
poetry run pylint ransformations tests
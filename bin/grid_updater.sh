#!/bin/bash

# source environment variables
echo "sourcing environment variables"
source /etc/environment
# set env variables

echo "set environment variables"

# shellcheck disable=SC1090
source $PRICING_SERVICE_BASE_PATH"/jobs/setup.sh"

# updating city and cargroup grids
echo "updating city and cargroup grids"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/util/grid_updater.py"
echo "completed"

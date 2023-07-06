#!/bin/bash

# source environment variables
echo "sourcing environment variables"
source /etc/environment

# set env variables

echo "set environment variables"

# shellcheck disable=SC1090
source $PRICING_SERVICE_BASE_PATH"/jobs/setup.sh"

echo "starting price monitoring service"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/services/pricing_monitor_service/monitoring_service.py"
echo "completed"

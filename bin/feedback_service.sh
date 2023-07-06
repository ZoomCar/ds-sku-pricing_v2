#!/bin/bash

# source environment variables
echo "sourcing environment variables"
source /etc/environment

# set env variables

echo "set environment variables"

# shellcheck disable=SC1090
source $PRICING_SERVICE_BASE_PATH"/jobs/setup.sh"

# run feedback service

echo "running feedback service"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/services/feedback_service/feedback_orchestrator.py"
echo "completed"
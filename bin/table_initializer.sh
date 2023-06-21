#!/bin/bash

# source environment variables
echo "sourcing environment variables"
source /etc/environment

# set env variables

echo "set environment variables"

# shellcheck disable=SC1090
source $PRICING_SERVICE_BASE_PATH"/jobs/setup.sh"

# create master config table
echo "starting master_config table creation process"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/entities/master_config.py"
echo "completed"

# create service execution tracker table
echo "starting service execution tracker table creation process"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/entities/service_execution_record.py"
echo "completed"

# create dpm table
echo "starting DPM table creation process"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/entities/dpm.py"
echo "completed"

# create pricing multipliers table
echo "starting pricing multipliers table creation process"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/entities/pricing_multipliers_range.py"
echo "completed"

# populate  master config table
echo "starting master config table population"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/util/initialize_master_config.py"
echo "completed"

# populate pricing multipliers table
echo "starting pricing multipliers table population"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/util/initialize_multiplier_range.py"
echo "completed"

# create Monitoring service table
echo "starting Monitoring service table creation process"
$PRICING_PYTHON_EXECUTABLE $PRICING_SERVICE_BASE_PATH"/entities/pricing_monitor.py"
echo "completed"

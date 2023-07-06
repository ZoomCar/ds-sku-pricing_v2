#!/bin/bash

# source environment variables
echo "sourcing environment variables"
source /etc/environment

echo "set environment variables"
source "$sku_pricing_service_base_path/.env"

echo "starting historical dpm persistence"
# shellcheck disable=SC2154
$sku_pricing_python_executable "$sku_pricing_service_base_path/src/services/pricing_service/historical_dpm_manager.py"
echo "completed"

#!/bin/bash

# source environment variables
echo "sourcing environment variables"
source /etc/environment

echo "set environment variables"
source "$sku_pricing_service_base_path/.env"

echo "starting pricing_service"
$sku_pricing_python_executable "$sku_pricing_service_base_path/src/services/pricing_service/main.py"
echo "service run complete"

# shellcheck disable=SC1090

source "$sku_pricing_service_base_path/bin/historical_dpm_manager.sh"

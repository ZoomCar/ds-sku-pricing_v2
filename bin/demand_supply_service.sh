echo "sourcing /etc/environment"
source /etc/environment

echo "set environment variables"
# shellcheck disable=SC2154
source "$sku_pricing_service_base_path/.env"

echo "starting demand supply service"
# shellcheck disable=SC2154
$sku_pricing_python_executable "$sku_pricing_service_base_path/src/services/demand_supply_service/demand_supply_service.py"
echo "service run complete"

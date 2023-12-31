from enum import Enum


class PricingMonitorStatus(Enum):
    ACTIVE_CITY_CARGROUP_NOT_GENERATED = "ACTIVE_CITY_CARGROUP_NOT_GENERATED"
    INACTIVE_CITY_CARGROUP_NOT_GENERATED = "INACTIVE_CITY_CARGROUP_NOT_GENERATED"
    ACTIVE_CITY_CARGROUP_DPM_OUTBOUND = "ACTIVE_CITY_CARGROUP_DPM_OUTBOUND"
    INACTIVE_CITY_CARGROUP_DPM_OUTBOUND = "INACTIVE_CITY_CARGROUP_UTIL_DPM"
    CITY_CARGROUP_DPM_NOT_UPDATED = "CITY_CARGROUP_DPM_NOT_UPDATED"
    PRICING_SERVICE_ERROR = "PRICING_SERVICE_ERROR"

import os

class Constants:
    monitoring_slack_url = "https://hooks.slack.com/services/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"
    feedback_slack_url = "https://hooks.slack.com/services/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"
    feedback_fill_threshold = 0.3
    feedback_table = "dynamic_pricing_feedback"
    price_multipliers_schema = "data_science"
    booking_history_days = 30
    utility_history_days = 45
    historical_price_multipliers_table = "pricing_multiplier_dynamic_pricing_historical"
    cargroup_demand_table = "dynamic_pricing_demand"
    supply_availability_table = "dynamic_pricing_supply"
    s3_bucket_name = "zoomcar-datascience"
    s3_folder_name = "dynamic_pricing/pricing_grids"
    app_version = "v3.0.0"
    padding_min_lead_time = 0
    padding_max_lead_time = 4320
    padding_min_duration = 0
    padding_max_duration = 10000
    monitoring_service_min_updated_minutes = 120
    monitoring_service_fallback_dpm = 1.0
    default_lead_time_buckets = [6, 10000]
    default_duration_buckets = [10, 24, 48, 10000]
    default_min_dpm = 0.8
    default_max_dpm = 1.2
    default_demand_supply_look_back = 168
    past_hr = 0 # it should always be zero in prod

    @staticmethod
    def city_cargroup_dpm_range_cols():
        return ['city_id', 'cargroup_id', 'min_dpm', 'max_dpm', 'config_status']

    @staticmethod
    def dynamic_pricing_cols():
        return ["id", "city_id", "car_id", "leadtime_start", "dynamic_pricing_dpm", "updated_at"]

    @staticmethod
    def pricing_multipliers_cols():
        return ["id", "run_id", "city_id", "cargroup_id", "leadtime_start", "leadtime_end", "duration_start",
                "duration_end", "dpm", "updated_at"]

    @staticmethod
    def get_historical_dpm_table_name():
        __tablename__ = 'sku_historical_dpm_v1'
        return __tablename__

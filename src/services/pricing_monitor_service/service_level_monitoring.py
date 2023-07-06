from datetime import datetime, timedelta

from src.constants.constants import Constants
from src.enums.pricing_monitor_status import PricingMonitorStatus
from src.models.pricing_monitor_model.pricing_monitor_model import PricingMonitorModel
from src.util.error_handlers import db_error_handler
from src.util.logger_provider import attach_logger
from src.util.trigger_slack_alert import slack_notification_trigger
from config import Config

@attach_logger
class ServiceMonitoringService:
    def __init__(self):
        self.__slack_url = Constants.monitoring_slack_url
        self.__user_name = "SERVICE_MONITORING_BOT"
        self.__min_updated_time = datetime.utcnow() - timedelta(
            minutes=int(Constants.monitoring_service_min_updated_minutes))

    def __get_published_dpm(self):
        self.__dynamic_dpm_df = PricingMonitorModel().get_dynamic_pricing_multipliers()

    @db_error_handler
    def __verify_city_car_id_updated(self):
        not_updated_mask = (self.__dynamic_dpm_df['updated_at'] <= self.__min_updated_time)
        not_updated_dpm_df = self.__dynamic_dpm_df[not_updated_mask]
        
        if not not_updated_dpm_df.empty:
            self.__trigger_slack_notification_alert(f"Service - SKU-DPM Environment - {Config.env} \n {PricingMonitorStatus.CITY_CARGROUP_DPM_NOT_UPDATED}")
            self.logger.warning(f'Pricing Service did not update dpm in the last {self.__min_updated_time} minutes')

    @db_error_handler
    def __verify_pricing_service_run(self):
        latest_pricing_run_record = PricingMonitorModel().get_latest_pricing_service_record()
        latest_run_finish_time = latest_pricing_run_record.end_time
        if (latest_run_finish_time is None) or (latest_run_finish_time <= self.__min_updated_time):
            self.__trigger_slack_notification_alert(PricingMonitorStatus.PRICING_SERVICE_ERROR)
            self.logger.warning(f'Pricing Service did not run in the last {self.__min_updated_time} minutes')

    def __trigger_slack_notification_alert(self, message):
        slack_notification_trigger(self.__slack_url, message, self.__user_name)

    def __verify_sku_service_run(self):
        latest_updated_time = max(self.__dynamic_dpm_df['updated_at'])
        if latest_updated_time <= self.__min_updated_time:
            self.__trigger_slack_notification_alert(f"Service - SKU-DPM Environment - {Config.env} \n SKU Pricing Service did not run in the last {self.__min_updated_time} minutes")
            self.logger.warning(f'SKU Pricing Service did not run in the last {self.__min_updated_time} minutes')

    @db_error_handler
    def run(self):
        self.__get_published_dpm()
        self.__verify_sku_service_run()


if __name__ == '__main__':
    ServiceMonitoringService().run()
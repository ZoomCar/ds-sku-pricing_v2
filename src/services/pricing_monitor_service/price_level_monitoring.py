import ast
import concurrent.futures
import multiprocessing
from datetime import datetime, timedelta

import pandas as pd

from src.constants.constants import Constants
from src.enums.pricing_monitor_status import PricingMonitorStatus
from src.models.pricing_monitor_model.pricing_monitor_model import PricingMonitorModel
from src.models.pricing_monitor_model.pricing_monitor_publisher import PricingMonitorPublisher
from src.util.error_handlers import db_error_handler
from src.util.logger_provider import attach_logger
from src.util.trigger_slack_alert import slack_notification_trigger


@attach_logger
class CityCarGroupLevelMonitoring():
    def __init__(self):
        self.__active_city_ids = self.__get_active_cities()
        self.__slack_url = Constants.monitoring_slack_url
        self.__user_name = "PRICE_MONITORING_BOT"
        self.__fallback_dpm = Constants.monitoring_service_fallback_dpm
        self.__alert_raised = False

    def __get_active_cities(self):
        return ast.literal_eval(self.__config["APP"]["active_city_ids"])

    def __get_published_dpm(self):
        self.__dynamic_dpm_df = PricingMonitorModel().get_dynamic_pricing_multipliers()

    @db_error_handler
    def __verify_active_city_dpm(self):
        active_city_dpm = self.__dynamic_dpm_df[self.__dynamic_dpm_df['city_id'].isin(self.__active_city_ids)]
        thread_count = multiprocessing.cpu_count() - 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_response = {executor.submit(self.__verify_active_city_dpm_outbounds, row): row
                                  for index, row in active_city_dpm.iterrows()}

    def __verify_active_city_dpm_outbounds(self, row):
        # config_status is to check whether the cargroup in city is active.
        if pd.isnull(row['dynamic_pricing_dpm']) and row['config_status'] is True:
            self.__trigger_slack_notification_alert()
            PricingMonitorPublisher().create_active_city_dpm_record(row['city_id'], row['cargroup_id'],
                                                                    self.__fallback_dpm)
            self.logger.info(
                f"DPM created for city_id: {row['city_id']} and cargroup_id: {row['cargroup_id']}")
        elif (((row['dynamic_pricing_dpm'] < row['min_dpm']) or (row['dynamic_pricing_dpm'] > row['max_dpm']))
              and (row['config_status'] is True)):
            self.__trigger_slack_notification_alert()
            PricingMonitorPublisher().update_dynamic_dpm_by_id(row['id'], self.__fallback_dpm,
                                                               row['dynamic_pricing_dpm'],
                                                               PricingMonitorStatus.ACTIVE_CITY_CARGROUP_DPM_OUTBOUND)

    @db_error_handler
    def __verify_inactive_city_dpm(self):
        inactive_cities_dpm_df = self.__dynamic_dpm_df[~self.__dynamic_dpm_df['city_id'].isin(self.__active_city_ids)]
        self.__inactive_cities = inactive_cities_dpm_df['city_id'].unique().tolist()
        self.__current_time = (datetime.utcnow() + timedelta(minutes=330)).replace(minute=0, second=0, microsecond=0)
        for city_id in self.__inactive_cities:
            city_util_dpm_df = PricingMonitorModel().get_utilisation_dpm(city_id)
            inactive_city_df = inactive_cities_dpm_df[inactive_cities_dpm_df['city_id'] == city_id]
            thread_count = multiprocessing.cpu_count() - 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
                future_to_response = {executor.submit(self.__verify_utilisation_multiplier_dpm, row,
                                                      city_util_dpm_df): row for index, row in
                                      inactive_city_df.iterrows()}
            for index, row in inactive_city_df.iterrows():
                self.__verify_utilisation_multiplier_dpm(row, city_util_dpm_df)

    def __verify_utilisation_multiplier_dpm(self, row, city_util_dpm_df):
        # config_status is to check whether the cargroup in city is active.
        if pd.isnull(row['dynamic_pricing_dpm']) and row['config_status'] is True:
            city_util_dpm_df['cargroup_id'] = row['cargroup_id']
            self.__generate_prices_inactive_city_dpm(city_util_dpm_df)
        elif row['config_status'] is True:
            start_time = pd.to_datetime(self.__current_time + timedelta(hours=row['leadtime_start']))
            start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            city_util_dpm_values = city_util_dpm_df.loc[pd.to_datetime(city_util_dpm_df['date']) ==
                                                        start_time, 'dpm'].tolist()
            city_util_dpm = city_util_dpm_values[0] if list(city_util_dpm_values) else self.__fallback_dpm
            if float(city_util_dpm) != float(row['dynamic_pricing_dpm']):
                self.__trigger_slack_notification_alert()
                PricingMonitorPublisher().update_dynamic_dpm_by_id(row['id'], city_util_dpm, row['dynamic_pricing_dpm'],
                                                                   PricingMonitorStatus.INACTIVE_CITY_CARGROUP_DPM_OUTBOUND)

    def __generate_prices_inactive_city_dpm(self, city_util_dpm_df):
        city_util_dpm_df['lead_time_start'] = (pd.to_datetime(city_util_dpm_df['date'])
                                               - self.__current_time) / pd.Timedelta(hours=1)
        city_util_dpm_df['lead_time_end'] = city_util_dpm_df['lead_time_start'] + 24
        city_util_dpm_df = city_util_dpm_df[city_util_dpm_df['lead_time_end'] >= 0]
        city_util_dpm_df.loc[city_util_dpm_df['lead_time_start'] < 0, 'lead_time_start'] = 0
        self.__trigger_slack_notification_alert()
        PricingMonitorPublisher().create_inactive_city_dpm_record(city_util_dpm_df)

    def __trigger_slack_notification_alert(self):
        if not self.__alert_raised:
            message = "Price level Monitoring Service triggered. Check Pricing Monitoring table for further details"
            slack_notification_trigger(self.__slack_url, message, self.__user_name)
            self.__alert_raised = True

    @db_error_handler
    def run(self):
        self.__get_published_dpm()
        self.__verify_active_city_dpm()
        self.__verify_inactive_city_dpm()


if __name__ == '__main__':
    CityCarGroupLevelMonitoring().run()

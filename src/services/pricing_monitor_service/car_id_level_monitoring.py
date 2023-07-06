import pandas as pd
from os import getenv

from src.util.logger_provider import attach_logger
from src.constants.constants import Constants
from src.models.supply_model import available_cars_query
from src.persistence.api.connection_pool_api import ConnectionPoolApi
from src.util.trigger_slack_alert import slack_notification_trigger

@attach_logger
class CarLevelMonitoringService:
    def __init__(self):
        self.__slack_url = Constants.monitoring_slack_url
        self.__user_name = "SERVICE_MONITORING_BOT"

    def __get_avaiable_car_ids(self):
        connection = ConnectionPoolApi.get_inventory_mysql_db_con_pool()
        available_car_ids = pd.read_sql_query(available_cars_query.get_query(), connection)
        return available_car_ids
 
    def __get_dynamic_pricing_multipliers(self):
        dpm_query = f"""select * from zoomcar_analytics.sku_dpm"""
        connection = ConnectionPoolApi.get_mysql_db_con_pool()
        all_cars_dpm = pd.read_sql_query(dpm_query, connection)
        return all_cars_dpm

    def __trigger_slack_notification_alert(self, message):
        slack_notification_trigger(self.__slack_url, message, self.__user_name)

    def __find_missing_car_ids(self, cars_df, dpm_df, leadtime_start, leadtime_end, duration_start, duration_end):
        flag_column = 'lead_{}_{}_duration_{}_{}'.format(leadtime_start, leadtime_end, duration_start, duration_end)
        cars_df[flag_column] = cars_df.apply(lambda row : ((dpm_df['car_id'] == row['av_inv_0_24']) & (dpm_df['leadtime_start'] == leadtime_start) & (dpm_df['leadtime_end'] == leadtime_end) & (dpm_df['duration_start'] == duration_start) & (dpm_df['duration_end'] == duration_end)).any(), axis=1)
        return list(cars_df.loc[cars_df[flag_column] == False]['av_inv_0_24'])

    def __check_dpm_for_car_ids(self):
        dpm_df = self.__get_dynamic_pricing_multipliers()
        cars_df = self.__get_avaiable_car_ids()
        leadtime_start = sorted(dpm_df['leadtime_start'].unique())
        leadtime_end = sorted(dpm_df['leadtime_end'].unique())
        duration_start = sorted(dpm_df['duration_start'].unique())
        duration_end = sorted(dpm_df['duration_end'].unique())
        for lt_idx in range(len(leadtime_start)):
            for dt_idx in range(len(duration_start)):
                missing_car_ids = self.__find_missing_car_ids(cars_df, dpm_df, leadtime_start[lt_idx], leadtime_end[lt_idx], duration_start[dt_idx], duration_end[dt_idx])
                if missing_car_ids:
                    message = "Service - SKU-DPM Enviroment - {} \n For the lead time {}-{} and duration {}-{}, dpm is not generated for the following car ids - {}".format(getenv("ENV"), leadtime_start[lt_idx], leadtime_end[lt_idx], duration_start[dt_idx], duration_end[dt_idx], missing_car_ids)
                    self.logger.info(message)
                    self.__trigger_slack_notification_alert(message)

    def run(self):
        self.__check_dpm_for_car_ids()


if __name__ == '__main__':
    CarLevelMonitoringService().run()

import itertools
import os
import pandas as pd
from sqlalchemy.exc import OperationalError
from src.constants.constants import Constants as c  # Constants is too verbose
from src.models.supply_model import available_cars_query, supply_query, active_cars_query
from src.persistence.api.connection_pool_api import ConnectionPoolApi
from src.util.logger_provider import attach_logger

"""
this is the main file that clients should access 
for supply data
"""

@attach_logger
class SupplyModel:
    def __init__(self):
        self.logger.info(f"starting supply model initialization")
        self.__slack_url = c.monitoring_slack_url
        self.__user_name = "PRICE_MONITORING_BOT"
        
        self.__active_car_df = self.__assign_active_car_ids()
        self.__supply_df = None
        # self.__supply_df = self.__assign_supply()
        self.logger.info(f"finished supply model initialization")
    
    def get_city_id_car_type_id_map(self):
        final_map = dict()
        for index, row in self.__assign_available_car_ids().iterrows():
            city_id = int(row["city_id"])
            car_type_id = int(row["car_type_id"])
            if city_id in final_map:  # if city id already there then append
                if car_type_id not in final_map[city_id]:  # avoid duplicate
                    final_map[city_id].append(car_type_id)
            else:  # add a list as value
                final_map[city_id] = [car_type_id]

        return final_map

    def __assign_supply(self):
        connection = ConnectionPoolApi.get_inventory_mysql_db_con_pool()
        try:
            supply_df = pd.read_sql_query(supply_query.get_query(), connection)
            return supply_df
        except OperationalError as e:
            if os.getenv("ENV") == "dev":
                self.logger.error(f"could not fetch supply data. This might be a vpn issue")
                return
            else:
                raise Exception(e)

    def __assign_available_car_ids(self):
        """
        available_car_id_df has columns:
        city, car_type, av_inv_0_24, av_inv_24_48, av_inv_48_72
        """
        connection = ConnectionPoolApi.get_inventory_mysql_db_con_pool()
        try:
            available_car_id_df = pd.read_sql_query(available_cars_query.get_query(), connection)
            return available_car_id_df
        except OperationalError as e:
            if os.getenv("ENV") == "dev":
                self.logger.error(f"could not fetch supply data. This might be a vpn issue")
                return
            else:
                raise Exception(e)

    def __assign_active_car_ids(self):
        """
        active_car_id_df has columns:
        car_id, city, car_type
        """
        connection = ConnectionPoolApi.get_inventory_mysql_db_con_pool()
        try:
            available_car_id_df = pd.read_sql_query(active_cars_query.get_query(), connection)
            return available_car_id_df
        except OperationalError as e:
            if os.getenv("ENV") == "dev":
                self.logger.error(f"could not fetch supply data. This might be a vpn issue")
                return
            else:
                raise Exception(e)
    
    def get_available_car_ids(self, city_id, car_type_id):
        if self.__active_car_df is None:
            if os.getenv("ENV") == "dev":
                self.logger.error(
                    f"available car id list is not generated, this could be a VPN related issue. Assigning dummy supply")
                car_id_list = [7541, 8628, 13316]
                return car_id_list
            else:
                self.logger.error("available car id list note generated")
                return []

        city_mask = self.__active_car_df["city_id"] == city_id
        car_type_mask = self.__active_car_df["car_type_id"] == car_type_id
        available_car = self.__active_car_df[city_mask & car_type_mask]
        if available_car.empty:
            self.logger.error(f"no available car data for city_id: {city_id} and car_type_id: {car_type_id}")
            return []

        available_car_id_list = [int(car_id) for car_id in available_car.car_id.values]
        self.logger.info(
            f"available car id list for city_id: {city_id} and car type id: {car_type_id}, {len(available_car_id_list)}, car_id_list: {available_car_id_list}")
        return available_car_id_list

    def get_supply(self, city_id, car_type_id):
        lead_time_buckets = c.default_lead_time_buckets
        duration_buckets = c.default_duration_buckets

        if self.__supply_df is None:
            self.logger.debug(f"supply data was not generated, this could be a VPN related issue. Assigning dummy supply")
            supply = {'lead_time': {
                lead_time_buckets[0]: {
                    'duration': {
                                duration_buckets[0]: 1, 
                                duration_buckets[1]: 1,
                                duration_buckets[2]: 1,
                                duration_buckets[3]: 1
                            }},
                lead_time_buckets[1]: {
                    'duration': {
                                duration_buckets[0]: 1, 
                                duration_buckets[1]: 1,
                                duration_buckets[2]: 1,
                                duration_buckets[3]: 1
                            }},
            }}
            return supply
        
        city_mask = self.__supply_df["city_id"] == city_id
        car_type_mask = self.__supply_df["car_type_id"] == car_type_id
        city_supply = self.__supply_df[city_mask & car_type_mask]
        if city_supply.empty:
            raise Exception(f"no supply data for city_id: {city_id} and car_type_id: {car_type_id}")
        supply_map = city_supply.iloc[0, :].to_dict()

        supply = {'lead_time': {
            lead_time_buckets[0]: {
                'duration': {
                    duration_buckets[0]: supply_map.get('av_0_10'),
                    duration_buckets[1]: supply_map.get('av_10_24'),
                    duration_buckets[2]: supply_map.get('av_24_48'),
                    duration_buckets[3]: supply_map.get('av_48_72'),
                    }},
            lead_time_buckets[1]: {
                'duration': {
                    duration_buckets[0]: supply_map.get('av_0_10'),
                    duration_buckets[1]: supply_map.get('av_10_24'),
                    duration_buckets[2]: supply_map.get('av_24_48'),
                    duration_buckets[3]: supply_map.get('av_48_72'),
                    }},
        }}

        return supply

    def get_supply_data_frame(self):
        return self.__supply_df


if __name__ == '__main__':
    obj = SupplyModel()
    cargroup_availability_data = obj.get_supply(1, 3)
    print(cargroup_availability_data)

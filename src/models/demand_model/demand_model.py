import os
import pandas

from config import Config
from src.constants.constants import Constants as c  # Constants is too verbose
from src.models.demand_model.demand_query import get_query
from src.persistence.api.athena_api import AthenaApi
from src.util.logger_provider import attach_logger


@attach_logger
class DemandModel:
    def __init__(self):
        self.logger.info(f"starting demand model initialization")
        self.__athena_api = AthenaApi(Config.reservoir_db_name, Config.s3_folder_name, Config.s3_bucket_name,
                                      Config.region_name)
        self.__demand_df = self.__assign_demand()
        self.logger.info(f"demand model initialization finished")

    def __assign_demand(self):
        query = get_query()
        try:
            demand_df = self.__athena_api.run_query(query)
            return demand_df
        except Exception as e:
            # self.logger.error(e)
            if os.getenv("ENV") == "dev":
                self.logger.error(f"could not fetch demand data. This might be a vpn issue")
                return
            else:
                raise Exception(e)
    
    def get_demand(self, city_id, car_type_id):
        lead_time_buckets = c.default_lead_time_buckets
        duration_buckets = c.default_duration_buckets

        if self.__demand_df is None:
            if os.getenv("ENV") == "dev":
                self.logger.error(
                    f"demand data was not generated, this could be a VPN related issue. Assigning dummy demand")
                demand = {'lead_time': {
                    lead_time_buckets[0]: {
                        'duration': {duration_buckets[0]: 100, 
                                    duration_buckets[1]: 330,
                                    duration_buckets[2]: 500, 
                                    duration_buckets[3]: 1000
                                    }},
                    lead_time_buckets[1]: {
                        'duration': {duration_buckets[0]: 100, 
                                    duration_buckets[1]: 330,
                                    duration_buckets[2]: 500, 
                                    duration_buckets[3]: 1000}},
                }}
                return demand
            else:
                raise Exception(f"couldn't generate demand")

        city_mask = self.__demand_df["city_id"] == city_id
        car_type_mask = self.__demand_df["car_type_id"] == car_type_id
        city_demand = self.__demand_df[city_mask & car_type_mask]
        if city_demand.empty:
            self.logger.warning(f"no demand data for city_id: {city_id} and car_type_id: {car_type_id}, assigning demand as 1")
            city_demand = pandas.DataFrame([{'city_id':city_id, 'car_type_id':car_type_id, 'JIT_0_10':1, 'JIT_10_24':1, 'JIT_0_24':1, 'JIT_24_48':1, 'JIT_48_10000':1, 'NJIT_0_10':1, 'NJIT_10_24':1, 'NJIT_0_24':1, 'NJIT_24_48':1, 'NJIT_48_10000':1}])            
        demand_map = city_demand.iloc[0, :].to_dict()

        demand = {'lead_time': {
            lead_time_buckets[0]: {
                'duration': {duration_buckets[0]: demand_map.get('JIT_0_10'),
                             duration_buckets[1]: demand_map.get('JIT_10_24'),
                             duration_buckets[2]: demand_map.get('JIT_24_48'),
                             duration_buckets[3]: demand_map.get('JIT_48_10000')}},
            lead_time_buckets[1]: {
                'duration': {duration_buckets[0]: demand_map.get('NJIT_0_10'),
                             duration_buckets[1]: demand_map.get('NJIT_10_24'),
                             duration_buckets[2]: demand_map.get('NJIT_24_48'),
                             duration_buckets[3]: demand_map.get('NJIT_48_10000')}},
        }}

        self.logger.info(f"demand for city_id: {city_id}, car_type_id: {car_type_id}: {demand}")
        return demand
    
    def get_demand_data_frame(self):
        return self.__demand_df

if __name__ == '__main__':
    demand_grid_generator_obj = DemandModel()
    demand_grid = demand_grid_generator_obj.get_demand(1, 3)
    print(demand_grid)

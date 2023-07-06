import concurrent.futures
import multiprocessing
import os
import random
from sqlalchemy.exc import OperationalError
from config import Config
from src.constants.constants import Constants
from src.entities.dpm import DPM
from src.entities.pricing_engine_grid_cell import PricingEngineGridCell
from src.entities.supplement_config import SupplementConfig
from src.enums.decay_type_enum import DecayType
from src.enums.utility_type_enum import UtilityType
from src.models.supply_model.supply_model import SupplyModel
from src.repositories.dpm_repository import DpmRepository
from src.repositories.supplement_config_repository import SupplementConfigRepository
from src.util.logger_provider import attach_logger
import pandas as pd

@attach_logger
class DpmPublisher:
    def __init__(self, city_id, car_type_id, supply_model, max_run_id):
        self.__city_id = city_id
        self.__car_type_id = car_type_id
        self.__dpm_repository = DpmRepository()
        self.__supply_model = supply_model
        self.__available_car_id = self.__supply_model.get_available_car_ids(self.__city_id, self.__car_type_id)
        self.__supplement_config = self.__assign_supplement_config()
        self.__current_max_run_id = max_run_id
        try:
            self.__discount_df = pd.read_csv(Config.discount_csv_path)
            self.logger.info('__discount_df: \n'+self.__discount_df.to_string(header=True, index=False))
        except Exception as e:
            self.logger.error(f"not able to read discount_csv_path: {Config.discount_csv_path}, so giving it as empty csv")
            self.logger.exception(e)
            self.__discount_df = pd.DataFrame()
        
    def __assign_supplement_config(self):
        try:
            supplement_config_list = SupplementConfigRepository().get_records_by_city_id_and_car_type_id(self.__city_id,
                                                                                                        self.__car_type_id)
            return supplement_config_list
        except OperationalError as e:
            if os.getenv("ENV") == "dev":
                self.logger.warning(
                    f"failed to load supplement config for city id {self.__city_id} and car type id {self.__car_type_id}, {e}")

                # create a dummy entry
                supplement_config_list = []
                for lead_time in Constants.default_lead_time_buckets:
                    for duration in Constants.default_duration_buckets:
                        supplement_config = SupplementConfig()
                        supplement_config.city_id = self.__city_id
                        supplement_config.car_type_id = self.__car_type_id
                        supplement_config.id = 1
                        supplement_config.max_dpm = 1.2
                        supplement_config.min_dpm = 0.8
                        supplement_config.lead_time = lead_time
                        supplement_config.duration = duration
                        supplement_config.decay_type = DecayType.LINEAR_DECAY.value
                        supplement_config.utility_type = UtilityType.SLOT_UTILITY.value
                        supplement_config_list.append(supplement_config)

                return supplement_config_list

            else:
                raise Exception(e)

    def __get_supplement_config_by_lead_time_duration(self, lead_time, duration) -> SupplementConfig:
        for record in self.__supplement_config:
            if record.lead_time == lead_time and record.duration == duration:
                return record

    def sprinkle_dpm(self, dpm):
        sprinkle = round(random.uniform(0.001, 0.005), 3)
        dpm = float(dpm) + sprinkle
        return dpm

    def __map_car_type_to_car_id(self, cell: PricingEngineGridCell):
        self.logger.info(
            f"mapping car_type_id {self.__car_type_id} lead time: {cell.lead_time} and duration: {cell.duration} to car_id level")
        
        __run_id = 1
        try:
            __run_id = int(self.__current_max_run_id) + 1
            self.logger.debug(f"setting current run id as: {__run_id}")
        except TypeError as e:
            self.logger.error(f"error while setting run_id: {e}, setting it to 1")
        
        # for individual car ids
        dpm_record_list = []
        for car_id in self.__available_car_id:
            dpm_record = DPM()
            dpm_record.city_id = self.__city_id
            dpm_record.car_id = car_id
            dpm_record.run_id = __run_id
            
            if cell.duration == 10:
                dpm_record.duration_start = 0
                dpm_record.duration_end = cell.duration
            elif cell.duration == 24:
                dpm_record.duration_start = 10
                dpm_record.duration_end = cell.duration
            elif cell.duration == 48:
                dpm_record.duration_start = 24
                dpm_record.duration_end = cell.duration
            elif cell.duration == 72:
                dpm_record.duration_start = 48
                dpm_record.duration_end = cell.duration
            elif cell.duration == 96:
                dpm_record.duration_start = 72
                dpm_record.duration_end = cell.duration
            elif cell.duration == 168:
                dpm_record.duration_start = 96
                dpm_record.duration_end = 168
            elif cell.duration == 10000:
                dpm_record.duration_start = 168
                dpm_record.duration_end = 10000
            else:
                self.logger.warning(
                    f"duration : {cell.duration} is not a valid value. setting 0 and 10000 as duration start and end values")
                dpm_record.duration_start = 0
                dpm_record.duration_end = 10000

            if cell.lead_time == 6:
                dpm_record.leadtime_start = 0
                dpm_record.leadtime_end = 6
            elif cell.lead_time == 10000:
                dpm_record.leadtime_start = 6
                dpm_record.leadtime_end = 10000

            else:
                self.logger.warning(
                    f"lead time : {cell.lead_time} is not a valid value. setting 0 and 10000 as lead time start and end values")

                dpm_record.leadtime_start = 0
                dpm_record.leadtime_end = 10000

            dpm_value = cell.dpm
            if len(self.__discount_df)>0:
                discount_multiplier = 1

                discount_values = self.__discount_df.loc[(self.__discount_df.city_id==dpm_record.city_id) & (self.__discount_df.car_id==dpm_record.car_id) & (self.__discount_df.duration_start==dpm_record.duration_start) & (self.__discount_df.duration_end==dpm_record.duration_end)].discount.values
                if len(discount_values)>0:
                    discount_multiplier = discount_values[0]
                else:
                    discount_values = self.__discount_df.loc[(self.__discount_df.city_id==dpm_record.city_id) & (self.__discount_df.car_id==dpm_record.car_id) & (self.__discount_df.duration_start==0) & (self.__discount_df.duration_end==10000)].discount.values
                    if len(discount_values)>0:
                        discount_multiplier = discount_values[0]
                
                if discount_multiplier!=1:
                    if discount_multiplier > 0.3 and discount_multiplier<3:
                        self.logger.info(f"discount for car_id: {dpm_record.car_id}, city_id: {dpm_record.city_id}, duration_start: {dpm_record.duration_start}, duration_end: {dpm_record.duration_end}  has discount: {discount_multiplier}")
                        self.logger.info(f'before: {dpm_value}')
                        dpm_value = dpm_value*discount_multiplier
                        self.logger.info(f'after: {dpm_value}')
                    else:
                        self.logger.error(f"discount isn't in correct range for car_id: {dpm_record.car_id}, city_id: {dpm_record.city_id}, duration_start: {dpm_record.duration_start}, duration_end: {dpm_record.duration_end} has discount: {discount_multiplier}")
            
            dpm_value = self.sprinkle_dpm(dpm_value)
            dpm_value = round(dpm_value, 3)
            dpm_record.dpm = dpm_value
            dpm_record_list.append(dpm_record)
        return dpm_record_list

    def handle_persistence(self, dpm_record_list):
        self.__dpm_repository.save_records(dpm_record_list)
        self.logger.info(
            f"successfully saved record for city_id: {self.__city_id}, car_type_id: {self.__car_type_id}, total new records: {len(dpm_record_list)}")

    def publish_grid(self, grid):
        self.logger.info(
            f"starting DPM publishing for city_id: {self.__city_id}, car_type_id: {self.__car_type_id}")
        car_id_level_dpm_record = [self.__map_car_type_to_car_id(cell) for cell in grid]
        
        thread_count = multiprocessing.cpu_count() - 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            [executor.submit(self.handle_persistence, dpm_record_list) for dpm_record_list in car_id_level_dpm_record]
        
        self.logger.info(
            f"successfully added new records for city_id: {self.__city_id}, car_type_id: {self.__car_type_id}")


if __name__ == "__main__":
    dpm_publisher = DpmPublisher(1, 3, SupplyModel())
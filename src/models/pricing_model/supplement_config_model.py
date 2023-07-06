import concurrent.futures
import multiprocessing

import pandas as pd

from src.entities.supplement_config import SupplementConfig
from src.repositories.master_config_repository import MasterConfigRepository
from src.repositories.supplement_config_repository import SupplementConfigRepository
from src.util.logger_provider import attach_logger


@attach_logger
class SupplementConfigModel:
    def __init__(self):
        self.__repository = SupplementConfigRepository()

    @staticmethod
    def __fetch_config_record_features(record):
        config_row = (
            record.id, record.city_id, record.car_type_id)
        return config_row

    def fetch_master_config_records(self):
        config_records = MasterConfigRepository().get_all_records()
        config_city_cargroup_df = pd.DataFrame(
            [self.__fetch_config_record_features(record) for record in config_records],
            columns=["id", "city_id", "car_type_id"])
        return config_city_cargroup_df

    @staticmethod
    def __create_price_multiplier_record(row):
        record = SupplementConfig()
        record.city_id = row['city_id']
        record.car_type_id = row['car_type_id']
        record.lead_time = row['lead_time']
        record.duration = row['duration']
        record.min_dpm = row["min_dpm"]
        record.max_dpm = row["max_dpm"]
        record.decay_type = row['decay_type']
        record.utility_type = row['utility_type']
        return record

    def publish_supplement_config(self, price_multiplier_df) -> None:
        records = [self.__create_price_multiplier_record(row) for index, row in price_multiplier_df.iterrows()]
        self.logger.info(f"price multipliers records created")
        thread_count = multiprocessing.cpu_count() - 1
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            executor.submit(self.__repository.save_records, records)
        self.logger.info(f"price multipliers records published successfully")


if __name__ == '__main__':
    city_cargroup_df = SupplementConfigModel().fetch_master_config_records()
    print(city_cargroup_df)

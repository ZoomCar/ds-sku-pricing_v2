import os

import pandas as pd
from sqlalchemy.exc import OperationalError

from src.constants.constants import Constants
from src.entities.supplement_config import SupplementConfig
from src.enums.decay_type_enum import DecayType
from src.enums.utility_type_enum import UtilityType
from src.repositories.supplement_config_repository import SupplementConfigRepository
from src.services.pricing_service.grid_generator import GridGenerator
from src.util.logger_provider import attach_logger


@attach_logger
class CityCarTypeGridModel:
    def __init__(self, city_id, car_type_id):
        self.__city_id = city_id
        self.__car_type_id = car_type_id

    @staticmethod
    def __fetch_price_multiplier_record_features(record):
        config_row = (record.id, record.city_id, record.car_type_id, record.lead_time, record.duration,
                      record.min_dpm, record.max_dpm, record.decay_type, record.utility_type)
        return config_row

    def _get_price_multipliers_range(self, ):
        try:
            multiplier_records = SupplementConfigRepository().get_records_by_city_id_and_car_type_id(self.__city_id,
                                                                                                     self.__car_type_id)
        except OperationalError as e:
            if os.getenv("ENV") == "dev":
                self.logger.warning(
                    f"failed to fetch supplement config records for city_id: {self.__city_id} and car_type_id: {self.__car_type_id} "
                    f"error: {e}. This is probably a VPN issue")

                multiplier_records = []
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
                        multiplier_records.append(supplement_config)

            else:
                raise Exception(e)

        self.logger.info(f"multiplier records : {multiplier_records}")
        self.__multiplier_df = pd.DataFrame(
            [self.__fetch_price_multiplier_record_features(record) for record in multiplier_records],
            columns=['multiplier_id', 'city_id', 'car_type_id', 'lead_time', 'duration', 'min_dpm', 'max_dpm',
                     'decay_type', 'utility_type'])

    def _update_grid_dpm_range(self, cell):
        mask = (self.__multiplier_df['lead_time'] == int(cell.lead_time)) & \
               (self.__multiplier_df['duration'] == int(cell.duration))
        cell.min_dpm = float(self.__multiplier_df.loc[mask, 'min_dpm'].values[0])
        cell.max_dpm = float(self.__multiplier_df.loc[mask, 'max_dpm'].values[0])
        cell.decay_type = self.__multiplier_df.loc[mask, 'decay_type'].values[0]
        return cell

    def build_city_cargroup_grid(self):
        self._get_price_multipliers_range()
        empty_grid = GridGenerator(self.__city_id, self.__car_type_id).get_grid()
        self.logger.info(
            f"Successfully generated empty grid for city_id: {self.__city_id} car_group_id: {self.__car_type_id}")
        # grid = [[self._update_grid_dpm_range(cell) for cell in elem] for elem in empty_grid]
        grid = [self._update_grid_dpm_range(cell) for cell in empty_grid]

        self.logger.info(f"Successfully updated grid for city_id: {self.__city_id} car_group_id: {self.__car_type_id}")
        return grid

if __name__ == '__main__':
    grid_model = CityCarTypeGridModel(1, 3)
    grid = grid_model.build_city_cargroup_grid()
    print(grid)

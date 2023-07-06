import os

from sqlalchemy.exc import OperationalError

from src.constants.constants import Constants
from src.entities.supplement_config import SupplementConfig
from src.enums.utility_type_enum import UtilityType
from src.repositories.supplement_config_repository import SupplementConfigRepository
from src.services.pricing_service.price_assigner_functions.slot_price_assigner import SlotPriceAssigner
from src.util.logger_provider import attach_logger


@attach_logger
class PriceAssigner:
    def __init__(self, city_id, car_type_id):
        self.__city_id = city_id
        self.__car_type_id = car_type_id
        self.__supplement_config = self.__assign_supplement_config()

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
                        supplement_config.utility_type = UtilityType.SLOT_UTILITY.value
                        supplement_config_list.append(supplement_config)
                return supplement_config_list

            else:
                raise Exception(e)

    def __get_supplement_config_by_lead_time_duration(self, lead_time, duration) -> SupplementConfig:
        for record in self.__supplement_config:
            if record.lead_time == lead_time and record.duration == duration:
                return record

    def assign_price(self, grid):
        updated_grid = []
        for elem in grid:
            supplement_config = self.__get_supplement_config_by_lead_time_duration(elem.lead_time, elem.duration)
            if supplement_config.utility_type == UtilityType.SLOT_UTILITY.value:
                price_assigned_grid = SlotPriceAssigner(self.__city_id, self.__car_type_id).assign_price(elem)
                updated_grid.append(price_assigned_grid)
            else:
                raise ValueError("Utility type calculation is not defined")
        return updated_grid


if __name__ == "__main__":
    price_assigner = PriceAssigner(6, 19)

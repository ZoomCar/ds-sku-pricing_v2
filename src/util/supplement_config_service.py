import pandas as pd

from src.constants.constants import Constants as c
from src.enums.decay_type_enum import DecayType
from src.enums.utility_type_enum import UtilityType
from src.models.pricing_model.supplement_config_model import SupplementConfigModel
from src.util.logger_provider import attach_logger


@attach_logger
class SupplementConfigService:
    def __init__(self):
        self.__supplement_config_model = SupplementConfigModel()

    @staticmethod
    def __build_leadtime_duration(row):
        """
        building a dataframe of all the combinations of lead_time and duration.
        currently row is of now use here
        """
        # in case city has custom lead time and duration bucket use that or take default values
        lead_time_list = c.default_lead_time_buckets
        duration_list = c.default_duration_buckets
        leadtime_duration = pd.MultiIndex.from_product([lead_time_list, duration_list], names=["lead_time", "duration"])
        return pd.DataFrame(index=leadtime_duration).reset_index()

    def worker(self):
        city_car_type_df = self.__supplement_config_model.fetch_master_config_records()
        for index, row in city_car_type_df.iterrows():
            price_multiplier_df = self.__build_leadtime_duration(row)
            price_multiplier_df['city_id'] = row['city_id']
            price_multiplier_df['car_type_id'] = row['car_type_id']
            price_multiplier_df['min_dpm'] = c.default_min_dpm
            price_multiplier_df['max_dpm'] = c.default_max_dpm
            price_multiplier_df['decay_type'] = DecayType.LINEAR_DECAY.value
            price_multiplier_df['utility_type'] = UtilityType.SLOT_UTILITY.value
            self.__supplement_config_model.publish_supplement_config(price_multiplier_df)
            self.logger.info(f"records saved for city_id: {row['city_id']} and car_type_id {row['car_type_id']}")


if __name__ == '__main__':
    SupplementConfigService().worker()

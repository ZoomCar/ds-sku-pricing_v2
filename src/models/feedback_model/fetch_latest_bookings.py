import pandas as pd
from src.repositories.master_config_repository import MasterConfigRepository
from src.repositories.feedback_repository import FeedbackRepository
from src.util.logger_provider import attach_logger


@attach_logger
class FetchBookingsModel:
    def __init__(self):
        self.__feedback_repository = FeedbackRepository()

    def __get_bookings_frequency_data(self):
        bookings_frequency_data = self.__feedback_repository.fetch_bookings_frequeny_data()
        self.logger.info("Bookings frequency data fetched")
        return bookings_frequency_data

    def __get_bookings_data(self):
        bookings_data = self.__feedback_repository.fetch_bookings_data()
        self.logger.info("Bookings data fetched")
        return bookings_data

    @staticmethod
    def __fetch_config_record_features(record):
        config_row = (record.city_id, record.car_group_id)
        return config_row

    def __fetch_master_config_records(self):
        config_records = MasterConfigRepository().get_all_records()
        config_city_cargroup_df = pd.DataFrame([self.__fetch_config_record_features(record)
                                                for record in config_records], columns=['city_id', 'cargroup_id'])
        self.logger.info("Master Config City and Cargroups fetched")
        return config_city_cargroup_df

    def __get_supply_data(self):
        supply_df = self.__feedback_repository.fetch_supply_data()
        self.logger.info("Supply data fetched")
        return supply_df

    def __get_latest_dpm(self):
        dpm_df = self.__feedback_repository.fetch_latest_dpm()
        self.logger.info("Latest DPM data fetched")
        return dpm_df

    def get_latest_booking_info(self):
        city_cargroup_df = self.__fetch_master_config_records()
        bookings_df = self.__get_bookings_data()
        bookings_frequency_df = self.__get_bookings_frequency_data()
        supply_df = self.__get_supply_data()
        dpm_df = self.__get_latest_dpm()
        merging_cols = ['city_id', 'cargroup_id', 'duration', 'leadtime']
        latest_booking_df = city_cargroup_df.merge(bookings_df, on=['city_id', 'cargroup_id'])
        latest_booking_df = latest_booking_df.merge(bookings_frequency_df, on=merging_cols)
        latest_booking_df = latest_booking_df.merge(supply_df, on=merging_cols)
        latest_booking_df = latest_booking_df.merge(dpm_df, on=merging_cols)
        self.logger.info("Latest bookings information fetched")
        return latest_booking_df


if __name__ == '__main__':
    feedback_model = FetchBookingsModel()

import numpy as np

from src.constants.constants import Constants
from src.models.feedback_model.feedback_publisher import FeedbackPublisher
from src.models.feedback_model.fetch_latest_bookings import FetchBookingsModel
from src.util.error_handlers import db_error_handler
from src.util.logger_provider import attach_logger
from src.util.trigger_slack_alert import slack_notification_trigger


@attach_logger
class FeeedbackOrchestrator:
    def __init__(self):
        self.__fill_threshold = float(Constants.feedback_fill_threshold)
        self.__slack_url = Constants.feedback_slack_url
        self.__user_name = "DPM_SANITISER_BOT"
        self.__message = f"""Lower fill rate for following city and cargoups: """

    def __get_latest_bookings_data(self):
        fetch_bookings_model = FetchBookingsModel()
        self.__latest_booking_data = fetch_bookings_model.get_latest_booking_info()
        self.__latest_booking_data.sort_values(by=['city_id', 'cargroup_id'], inplace=True)
        self.logger.info("latest booking data fetched")

    def __get_city_cargroups_fill_rate(self):
        self.__get_latest_bookings_data()
        self.__latest_booking_data['city_bookings'] = self.__latest_booking_data.groupby(['city_id'])[
            'bookings'].transform('sum')
        self.__latest_booking_data['car_utility_ratio'] = np.where(self.__latest_booking_data['start_supply'] != 0,
                                                                   self.__latest_booking_data['bookings'] /
                                                                   self.__latest_booking_data['start_supply'], 0)
        self.__latest_booking_data['booking_ratio'] = np.where(self.__latest_booking_data['city_bookings'] != 0,
                                                               self.__latest_booking_data['bookings'] /
                                                               self.__latest_booking_data['city_bookings'], 0)
        self.__latest_booking_data['fill_ratio'] = np.where(self.__latest_booking_data['car_utility_ratio'] != 0,
                                                            self.__latest_booking_data['booking_ratio'] /
                                                            self.__latest_booking_data['car_utility_ratio'], 0)
        self.__latest_booking_data['car_utility_ratio'].fillna(0, inplace=True)
        self.__latest_booking_data['booking_ratio'].fillna(0, inplace=True)
        self.__latest_booking_data['fill_ratio'].fillna(0, inplace=True)
        self.__latest_booking_data.sort_values(by=['city_id', 'fill_ratio'], inplace=True)
        self.logger.info("Calculated fill rate for city and cargoups")

    def __raise_slack_alerts(self, latest_booking_data):
        for index, row in latest_booking_data.iterrows():
            self.__message = self.__message + f"\n city_id: {row.city_id}, cargroup_id: {row.cargroup_id}, " \
                                              f"duration: {row.duration}, leadtime: {row.leadtime}, supply: {row.start_supply}, " \
                                              f"hours_since_last_booking: {row.hours_since_last_booking}, DPM: {row.dpm}"
        slack_notification_trigger(self.__slack_url, str(self.__message), self.__user_name)
        self.logger.info("successfully raised feedback alert")

    def __publish_low_fill_rate_city_cargroups(self):
        if len(self.__latest_booking_data):
            self.__raise_slack_alerts(self.__latest_booking_data)
        FeedbackPublisher().publish_feedback_data(self.__latest_booking_data)

    @db_error_handler
    def run(self):
        self.logger.info("started Feedback orchestrator")
        self.__get_city_cargroups_fill_rate()
        self.__publish_low_fill_rate_city_cargroups()


if __name__ == "__main__":
    orchestrator = FeeedbackOrchestrator()
    orchestrator.run()

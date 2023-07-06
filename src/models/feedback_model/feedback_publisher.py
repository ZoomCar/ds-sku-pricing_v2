from datetime import datetime
from src.repositories.service_execution_tracker_repository import ServiceExecutionTrackerRepository
from src.repositories.feedback_repository import FeedbackRepository
from src.util.logger_provider import attach_logger


@attach_logger
class FeedbackPublisher:
    def __init__(self):
        self.__feedback_repository = FeedbackRepository()

    @staticmethod
    def __transform_bookings_data(latest_booking_data):
        latest_booking_data['city_id'] = latest_booking_data['city_id'].astype(int)
        latest_booking_data['cargroup_id'] = latest_booking_data['cargroup_id'].astype(int)
        latest_booking_data['hours_since_last_booking'] = latest_booking_data['hours_since_last_booking'].astype(int)
        latest_booking_data['start_supply'] = latest_booking_data['start_supply'].astype(int)
        latest_booking_data['end_supply'] = latest_booking_data['end_supply'].astype(int)
        latest_booking_data['city_bookings'] = latest_booking_data['city_bookings'].astype(int)
        latest_booking_data['bookings'] = latest_booking_data['bookings'].astype(int)
        latest_booking_data['run_id'] = ServiceExecutionTrackerRepository().get_max_id()
        latest_booking_data['created_at'] = datetime.now()
        return latest_booking_data

    def publish_feedback_data(self, latest_booking_data):
        latest_booking_data = self.__transform_bookings_data(latest_booking_data)
        
        self.__feedback_repository.save_feedback_data(latest_booking_data)
        self.logger.info("feedback data published successfully")


if __name__ == '__main__':
    feedback_publisher = FeedbackPublisher()

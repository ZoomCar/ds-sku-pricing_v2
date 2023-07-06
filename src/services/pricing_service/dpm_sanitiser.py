from src.constants.constants import Constants
from src.entities.pricing_engine_grid_cell import PricingEngineGridCell
from src.util.logger_provider import attach_logger
from src.util.trigger_slack_alert import slack_notification_trigger


@attach_logger
class DpmSanitiser:
    def __init__(self, city_id, car_type_id):
        self.__city_id = city_id
        self.__car_type_id = car_type_id
        self.__slack_url = Constants.monitoring_slack_url
        self.__user_name = "DPM_SANITISER_BOT"
        self.__message = f"""following slots are out of bound for city_id: {self.__city_id} and car_type_id: {self.__car_type_id}"""
        self.__raise_alert = False

    def __trigger_slack_notification_alert(self):
        if self.__raise_alert:
            slack_notification_trigger(self.__slack_url, str(self.__message), self.__user_name)
            self.logger.info(
                f"successfully raised sanitiser alert for city_id: {self.__city_id}, car_type_id: {self.__car_type_id}")

    def __sanitise_dpm(self, cell: PricingEngineGridCell):
        if cell.dpm < float(cell.min_dpm):  # if dpm is lower than minimum, change it to the minimum value
            self.__raise_alert = True
            self.__message = self.__message + f"\n lead_time: {cell.lead_time}, duration: {cell.duration}, " \
                                              f"erroneous_dpm: {cell.dpm}, revised_dpm: float({cell.min_dpm}) "
            self.logger.warning(f"erroneous cell: {cell}")
            cell.dpm = float(cell.min_dpm)
        if cell.dpm > cell.max_dpm:  # if dpm is higher than maximum, reset it to maximum value.
            self.__raise_alert = True
            self.__message = self.__message + f"\n lead_time: {cell.lead_time}, duration: {cell.duration}, " \
                                              f"erroneous_dpm: {cell.dpm}, revised_dpm: float({cell.max_dpm}) "
            self.logger.warning(f"erroneous cell: {cell}")
            cell.dpm = float(cell.max_dpm)
        return cell

    def get_grid(self, grid):
        # self.__initialize()
        dpm_sanitised_grid = [self.__sanitise_dpm(elem) for elem in grid]
        self.logger.info(
            f"successfully sanitized grid for city_id: {self.__city_id}, car_type_id: {self.__car_type_id}")
        # self.__trigger_slack_notification_alert() # commenting the code to raise alerts for now
        return dpm_sanitised_grid


if __name__ == "__main__":
    dpm_sanitiser = DpmSanitiser(54, 19)

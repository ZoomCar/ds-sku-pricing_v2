from email import message

from matplotlib.pyplot import table
from src.util.logger_provider import attach_logger
from config import Config
from src.util.trigger_slack_alert import slack_notification_trigger
from src.constants.constants import Constants
import requests

@attach_logger
class DpmMappingPublisher:
    def __init__(self):
        self.__slack_url = Constants.monitoring_slack_url
        self.__user_name = "SERVICE_SKU_PRICING_BOT"
        self.__sku_dpm_mapping_bulk_add_api_url = Config.sku_dpm_mapping_bulk_add_api_url
        self.__sku_dpm_mapping_delete_api_url = Config.sku_dpm_mapping_delete_api_url
        self.__api_headers = { 
            "email": Config.api_email, 
            "token": Config.api_token
        }
    
    def __trigger_slack_notification_alert(self, message):
        slack_notification_trigger(self.__slack_url, message, self.__user_name)
        
    def bulk_add_data(self, dpm_mapping_data):
        url=self.__sku_dpm_mapping_bulk_add_api_url

        self.logger.info(f"started bulk_add_data at url: {url} for dpm_mapping_data: {dpm_mapping_data}")
        api_response = requests.post(url=url, headers=self.__api_headers, json = dpm_mapping_data)
        if api_response.status_code != 200:
            api_response = requests.post(url=url, headers=self.__api_headers, json = dpm_mapping_data)
        if api_response.status_code != 200:
            api_response = requests.post(url=url, headers=self.__api_headers, json = dpm_mapping_data)
        
        if api_response.status_code == 200:
            self.logger.debug("bulk_add request api is successful")
            self.logger.info("finished bulk_add_data")
            return True
        else:
            __error_message = f"bulk add request api is failed for dpm_mapping_data: {dpm_mapping_data}, response.status_code: {api_response.status_code}, response.reason: {api_response.reason}, response.text:{api_response.text}"
            self.__trigger_slack_notification_alert(__error_message)
            if api_response.status_code==504:
                self.logger.debug(__error_message)
            else:
                self.logger.error(__error_message)
            return False
    
    def delete_data(self, city_id, run_id):
        url=self.__sku_dpm_mapping_delete_api_url
        
        self.logger.info(f"started delete_data for city_id: {city_id}, run_id: {run_id} url: {url}")
        api_response = requests.delete(
            url=url+f"?runId={int(run_id)}&cityId={city_id}", 
            headers=self.__api_headers
        )
        
        if api_response.status_code != 200:
            api_response = requests.delete(
            url=url+f"?runId={int(run_id)}&cityId={city_id}", 
            headers=self.__api_headers)
        if api_response.status_code != 200:
            api_response = requests.delete(
            url=url+f"?runId={int(run_id)}&cityId={city_id}", 
            headers=self.__api_headers)
        
        if api_response.status_code == 200:
            self.logger.debug("delete request api is successful")
        else:
            __error_message = f"delete request api is failed for city_id: {city_id}, run_id: {run_id}, response.status_code: {api_response.status_code}, response.reason: {api_response.reason}, response.text:{api_response.text}"
            self.__trigger_slack_notification_alert(__error_message)
            if api_response.status_code==504:
                self.logger.debug(__error_message)
            else:
                self.logger.error(__error_message)

        self.logger.info("finished delete_data")

if __name__ == "__main__":
    dpm_mapping_publisher = DpmMappingPublisher()
    data = [
            {
                "carId":1,
                "cityId": 1,
                "runId":100,
                "multiplier":1.01,
                "leadTimeStart":0,
                "leadTimeEnd":6,
                "durationStart":6,
                "durationEnd":24
                
            },
            {
                "carId":2,
                "cityId": 1,
                "runId":100,
                "multiplier":1.03,
                "leadTimeStart":0,
                "leadTimeEnd":6,
                "durationStart":6,
                "durationEnd":24
                
            },
            {
                "carId":3,
                "cityId": 1,
                "runId":100,
                "multiplier":1.05,
                "leadTimeStart":0,
                "leadTimeEnd":6,
                "durationStart":6,
                "durationEnd":24
                
            },
            {
                "carId":4,
                "cityId": 1,
                "runId":100,
                "multiplier":1.08,
                "leadTimeStart":0,
                "leadTimeEnd":6,
                "durationStart":6,
                "durationEnd":24
                
            },
            {
                "carId":5,
                "cityId": 1,
                "runId":100,
                "multiplier":1.09,
                "leadTimeStart":0,
                "leadTimeEnd":6,
                "durationStart":6,
                "durationEnd":24
                
            },
            {
                "carId":6,
                "cityId": 1,
                "runId":100,
                "multiplier":1.1,
                "leadTimeStart":0,
                "leadTimeEnd":6,
                "durationStart":6,
                "durationEnd":24
                
            }
        ]
    # dpm_mapping_publisher.bulk_add_data(data)
    dpm_mapping_publisher.delete_data(city_id=1, run_id=100)

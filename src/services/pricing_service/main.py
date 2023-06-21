from datetime import datetime
import pandas as pd
from sqlalchemy.exc import OperationalError
from src.entities.master_config import MasterConfig
from src.models.demand_model.demand_model import DemandModel
from src.models.pricing_model.city_car_type_grid_model import CityCarTypeGridModel
from src.models.pricing_model.dpm_publisher import DpmPublisher
from src.models.supply_model.supply_model import SupplyModel
from src.repositories.dpm_repository import DpmRepository
from src.repositories.master_config_repository import MasterConfigRepository
from src.services.pricing_service.dpm_sanitiser import DpmSanitiser
from src.services.pricing_service.price_assigner import PriceAssigner
from src.services.pricing_service.utility_assigner import UtilityAssigner
from src.util.file_util import remove_temporary_folders
from src.util.logger_provider import attach_logger
from src.constants.constants import Constants as c
import copy
import numpy as np
from src.repositories.cargroup_availability_repository import CargroupAvailabilityRepository
from src.util.trigger_slack_alert import slack_notification_trigger
from src.constants.constants import Constants
from src.models.pricing_model.dpm_mapping_publisher import DpmMappingPublisher
from src.util.error_handlers import db_error_handler

@attach_logger
class PriceGenerator:
    def __init__(self):
        self.logger.info(f"orchestrator initialization started")
        self.__master_config_repository = MasterConfigRepository()
        self.__dpm_repository = DpmRepository()
        self.__start_time = datetime.utcnow()
        self.__supply_model = SupplyModel()
        self.__demand_model = DemandModel()
        self.__max_run_id = self.__dpm_repository.get_max_run_id()
        self.logger.info(f"orchestrator initialization finished, existing max run id: {self.__max_run_id}")
        self.__slack_url = Constants.monitoring_slack_url
        self.__user_name = "SERVICE_SKU_PRICING_BOT"
        self.__dpm_mapping_publisher = DpmMappingPublisher()
    
    def __trigger_slack_notification_alert(self, message):
        slack_notification_trigger(self.__slack_url, message, self.__user_name)
        return
    
    def _generate_prices(self, city_id, car_type_id):
        self.logger.info(f"triggering new pricing service for city_id: {city_id} car_group_id: {car_type_id}")
        empty_grid = CityCarTypeGridModel(city_id, car_type_id).build_city_cargroup_grid()  #sorted
        self.logger.info(f"Successfully generated grid for city_id: {city_id} car_group_id: {car_type_id}")
        utility_assigned_grid = UtilityAssigner(city_id, car_type_id, self.__supply_model,
                                                self.__demand_model).assign_utility(empty_grid)
        price_assigned_grid = PriceAssigner(city_id, car_type_id).assign_price(utility_assigned_grid)
        sanitized_grid = DpmSanitiser(city_id, car_type_id).get_grid(price_assigned_grid)
        if int(city_id) == 66:
            sanitized_grid_final = []
            for dpm_record in sanitized_grid:
                if dpm_record.duration==10000:
                    dpm_record_72 = copy.deepcopy(dpm_record)
                    dpm_record_72.duration = 72
                    sanitized_grid_final.append(dpm_record_72)

                    dpm_record_96 = copy.deepcopy(dpm_record)
                    dpm_record_96.duration = 96
                    sanitized_grid_final.append(dpm_record_96)
                    
                    dpm_record_168 = copy.deepcopy(dpm_record)
                    dpm_record_168.duration = 168
                    sanitized_grid_final.append(dpm_record_168)

                    dpm_record_10000 = copy.deepcopy(dpm_record)
                    sanitized_grid_final.append(dpm_record_10000)
                else:
                    sanitized_grid_final.append(dpm_record)
        else:
            sanitized_grid_final = []
            for dpm_record in sanitized_grid:
                if dpm_record.duration==10000:
                    dpm_record_72 = copy.deepcopy(dpm_record)
                    dpm_record_72.duration = 72
                    sanitized_grid_final.append(dpm_record_72)

                    dpm_record_96 = copy.deepcopy(dpm_record)
                    dpm_record_96.duration = 96
                    dpm_record_96.dpm = round(dpm_record_96.dpm*0.93,2)
                    sanitized_grid_final.append(dpm_record_96)
                    
                    dpm_record_168 = copy.deepcopy(dpm_record)
                    dpm_record_168.duration = 168
                    dpm_record_168.dpm = round(dpm_record_168.dpm*0.88,2)
                    sanitized_grid_final.append(dpm_record_168)

                    dpm_record_10000 = copy.deepcopy(dpm_record)
                    dpm_record_10000.dpm = round(dpm_record_10000.dpm*0.83,2)
                    sanitized_grid_final.append(dpm_record_10000)
                else:
                    sanitized_grid_final.append(dpm_record)
        
        DpmPublisher(city_id, car_type_id, self.__supply_model, self.__max_run_id).publish_grid(sanitized_grid_final)

    def _price_handler(self, city_id):
        # delete city records from DPM table first
        response = self.__dpm_repository.delete_records_by_city(city_id)  # response is of type Query
        self.logger.info(
            f"successfully deleted existing records for city_id: {city_id} total deleted records: {len(response.all())}")
        try:
            record_list = self.__master_config_repository.get_record_by_city_id(city_id)
        except OperationalError as e:
            master_config = MasterConfig()
            master_config.id = 1
            master_config.city_id = 1
            master_config.car_type_id = 3
            master_config.status = 1
            record_list = [master_config]
        # record_list has elements of type MasterConfig
        response = [self._generate_prices(record.city_id, record.car_type_id) for record in
                    record_list]

    @db_error_handler
    def _syncing_sku_dpm_mapping(self, all_active_city_list):
        self.logger.info(f"started _syncing_sku_dpm_mapping")
        
        all_cities = all_active_city_list
        self.logger.info(f"all_cities: {all_cities}")

        for city_id in all_cities:
            try:
                self.logger.info(f'sku_dpm_mapping_API:started publishing data in sku_dpm_mapping using API for city_id: {city_id}')
                dpm_mapping_df, run_ids = self.__dpm_repository.get_sku_dpm_of_city(city_id=city_id)
                run_ids_inv = CargroupAvailabilityRepository.get_sku_dpm_mapping_run_ids_of_city(city_id=city_id)
                
                dpm_mapping_data = list(dpm_mapping_df.to_dict(orient='index').values())
                run_ids = dpm_mapping_df.runId.values
                self.logger.info(f'sku_dpm_mapping_API: len(dpm_mapping_data): {len(dpm_mapping_data)}')
                if len(dpm_mapping_data)==0:
                    self.logger.warning(f"sku_dpm_mapping_API: len(dpm_mapping_data) is 0 for city: {city_id}")
                    continue
                
                batch_size = 1000
                bulk_add_api_response_flags = []
                for idx_dpm_mapping_data in range(0, len(dpm_mapping_data), batch_size):
                    bulk_add_api_response_flags.append(self.__dpm_mapping_publisher.bulk_add_data(dpm_mapping_data=dpm_mapping_data[idx_dpm_mapping_data:idx_dpm_mapping_data+batch_size]))
                
                if len(run_ids_inv)>=1  and min(bulk_add_api_response_flags):
                    # run_id_delete = min(run_ids_inv)
                    for run_id_delete in sorted(run_ids_inv)[:-1]:
                        if run_ids[0]!=run_id_delete:
                            self.logger.info(f'sku_dpm_mapping_API: run_id_delete: {run_id_delete}')
                            self.__dpm_mapping_publisher.delete_data(city_id=city_id, run_id=run_id_delete)
                else:
                    self.logger.error(f"sku_dpm_mapping_API: bulk_add_api_response_flags was false atlease once or deleteFlag was True, that's why we're not deleting sku_dpm_mapping data for city_id:{city_id}")
                self.logger.info(f'sku_dpm_mapping_API: finished publishing data in sku_dpm_mapping using API for city_id: {city_id}')
            except Exception as e:
                self.__trigger_slack_notification_alert(f"sku_dpm_mapping_API: ERROR for city: {city_id}: {str(e)}")
                self.logger.exception(e)

        self.logger.info(f"finished _syncing_sku_dpm_mapping")

    # @db_error_handler
    def run(self):
        remove_temporary_folders()
        self.logger.info("started price generation")
        
        try:
            self.logger.info(f"fetching all active city list")
            all_active_city_list = self.__dpm_repository.get_all_city_ids()
        except OperationalError as e:
            self.logger.warning(
                f"failed to load master config {e}. This is probably a VPN issue")
                
        self.logger.info(f"prices will be generated for these city ids: {all_active_city_list}")
        response = [self._price_handler(city_id) for city_id in all_active_city_list]
        
        self._syncing_sku_dpm_mapping(all_active_city_list)

if __name__ == "__main__":
    price_generator = PriceGenerator()
    price_generator.run()

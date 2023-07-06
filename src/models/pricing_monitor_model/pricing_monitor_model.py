import pandas as pd

from src.repositories.dpm_repository import DpmRepository
from src.repositories.master_config_repository import MasterConfigRepository
from src.repositories.utilisation_dpm_repository import UtilDpmRepository
from src.repositories.service_execution_tracker_repository import ServiceExecutionTrackerRepository
from src.constants.constants import Constants

class PricingMonitorModel():
	def __init__(self):
		pass

	def __fetch_utilisation_multipliers(self, city_id):
		self.__pricing_delta_df = UtilDpmRepository.get_pricing_delta(city_id)
		self.__pricing_multipliers_df = UtilDpmRepository.get_price_multiplier(city_id)

	def __build_utilisation_dpm_df(self, city_id):
		self.__fetch_utilisation_multipliers(city_id)
		self.__util_dpm_df = self.__pricing_delta_df.merge(self.__pricing_multipliers_df, on=['city_id'], how='left')
		delta_mask = (self.__util_dpm_df['delta'] >= self.__util_dpm_df['start_delta']) & (
								self.__util_dpm_df['delta'] < self.__util_dpm_df['end_delta'])
		self.__util_dpm_df = self.__util_dpm_df[delta_mask]

	def __fetch_config_record_features(self, record):
		config_row = (record.city_id, record.car_group_id, record.min_dpm, record.max_dpm, record.status)
		return config_row

	def __fetch_master_config_records(self):
		config_records = MasterConfigRepository().get_all_records()
		self.__config_city_cargroup_df = pd.DataFrame([self.__fetch_config_record_features(record) for record in config_records],
                                                      columns=Constants.city_cargroup_dpm_range_cols())

	def __fetch_dynamic_pricing_features(self, record):
		dynamic_pricing_row = (record.id, record.city_id, record.car_id, record.leadtime_start, 
												record.dpm, record.updated_at)
		return dynamic_pricing_row

	def __fetch_dynamic_pricing_multipliers(self):
		dynamic_dpm_list = DpmRepository().get_all_records()
		self.__dynamic_dpm_df = pd.DataFrame([self.__fetch_dynamic_pricing_features(record) for record in dynamic_dpm_list],
                                             columns=Constants.dynamic_pricing_cols())
		
	def get_latest_pricing_service_record(self):
		latest_run_id = ServiceExecutionTrackerRepository().get_max_id()
		latest_run_record = ServiceExecutionTrackerRepository().get_record_by_id(latest_run_id)
		return latest_run_record

	def get_utilisation_dpm(self, city_id):
		self.__build_utilisation_dpm_df(city_id)
		util_dpm_df = self.__util_dpm_df
		return util_dpm_df

	def get_dynamic_pricing_multipliers(self):
		self.__fetch_dynamic_pricing_multipliers()
		dynamic_dpm_df = self.__dynamic_dpm_df
		return dynamic_dpm_df

if __name__ == '__main__':
    dynamic_dpm_df = PricingMonitorModel().get_dynamic_pricing_multipliers()
    util_dpm_df = PricingMonitorModel().get_utilisation_dpm(1)
    

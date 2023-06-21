from datetime import datetime

import pandas as pd

from src.constants.constants import Constants
from src.entities.dpm import DPM
from src.entities.pricing_monitor import PricingMonitor
from src.enums.pricing_monitor_status import PricingMonitorStatus
from src.repositories.dpm_repository import DpmRepository
from src.repositories.pricing_monitoring_repository import PricingMonitoringRepository
from src.repositories.service_execution_tracker_repository import ServiceExecutionTrackerRepository


class PricingMonitorPublisher():
    def __init__(self):
        self.__dpm_repository = DpmRepository()
        self.__monitoring_repository = PricingMonitoringRepository()

    def update_dynamic_dpm_by_id(self, id: int, changed_dpm: float, generated_dpm: float,
                                 status: PricingMonitorStatus) -> None:
        updated_time = datetime.utcnow()
        update_payload = {"updated_at": updated_time, "dpm": float(changed_dpm)}
        self.__dpm_repository.update_record_by_id(id, update_payload)
        record = self.__dpm_repository.get_record_by_id(id)
        self.__create_pricing_monitor_record([record], generated_dpm, status)

    def create_active_city_dpm_record(self, city_id: int, cargroup_id: int, dpm: float) -> None:
        record = DPM()
        record.run_id = ServiceExecutionTrackerRepository().get_max_id()
        record.city_id = city_id
        record.cargroup_id = cargroup_id
        record.leadtime_start = int(Constants.padding_min_lead_time)
        record.leadtime_end = int(Constants.padding_max_lead_time)
        record.duration_start = int(Constants.padding_min_duration)
        record.duration_end = int(Constants.padding_max_duration)
        record.dpm = float(dpm)
        self.__create_pricing_monitor_record([record], None, PricingMonitorStatus.ACTIVE_CITY_CARGROUP_NOT_GENERATED)
        self.__dpm_repository.save_record(record)

    def __transform_inactive_city_dpm(self, row):
        record = DPM()
        record.run_id = ServiceExecutionTrackerRepository().get_max_id()
        record.city_id = row['city_id']
        record.cargroup_id = row['cargroup_id']
        record.leadtime_start = row['lead_time_start']
        record.leadtime_end = row['lead_time_end']
        record.duration_start = int(Constants.padding_min_duration)
        record.duration_end = int(Constants.padding_max_duration)
        record.dpm = float(row['dpm'])
        return record

    def create_inactive_city_dpm_record(self, util_dpm: pd.DataFrame) -> None:
        records = list(util_dpm.apply(lambda row: self.__transform_inactive_city_dpm(row), axis=1))
        self.__create_pricing_monitor_record(records, None, PricingMonitorStatus.INACTIVE_CITY_CARGROUP_NOT_GENERATED)
        
        self.__dpm_repository.save_records(records)

    def __transform_pricing_monitor_record(self, dpm_record, generated_dpm, pricing_monitor_status):
        record = PricingMonitor()
        record.run_id = dpm_record.run_id
        record.city_id = dpm_record.city_id
        record.car_group_id = dpm_record.cargroup_id
        record.leadtime_start = dpm_record.leadtime_start
        record.leadtime_end = dpm_record.leadtime_end
        record.duration_start = dpm_record.duration_start
        record.duration_end = dpm_record.duration_end
        record.generated_dpm = generated_dpm
        record.changed_dpm = dpm_record.dpm
        record.status = pricing_monitor_status.value
        return record

    def __create_pricing_monitor_record(self, dpm_records: DPM, generated_dpm: float,
                                        status: PricingMonitorStatus) -> None:
        records = [self.__transform_pricing_monitor_record(dpm_record, generated_dpm, status)
                   for dpm_record in dpm_records]
        
        self.__monitoring_repository.save_records(records)

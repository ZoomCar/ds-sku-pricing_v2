from datetime import datetime

from src.constants.constants import Constants
from src.entities.service_execution_record import ServiceExecutionRecord
from src.enums.service_execution_status import ServiceExecutionStatus
from src.repositories.service_execution_tracker_repository import ServiceExecutionTrackerRepository


class ServiceExecutionRecordPublisher:
    def __init__(self):
        self.__repository = ServiceExecutionTrackerRepository()

    def create_service_execution_start_record(self, start_time) -> int:
        record = ServiceExecutionRecord()
        record.start_time = start_time
        record.version = Constants.app_version
        record.status = ServiceExecutionStatus.STARTED.value
        return self.__repository.save_record(record)

    def update_service_execution_record(self, id: int, status: ServiceExecutionStatus) -> None:
        end_time = datetime.utcnow()
        # @todo add enums for hard-coded keys
        update_payload = {"end_time": end_time, "status": status.value}
        self.__repository.update_record_by_id(id, update_payload)

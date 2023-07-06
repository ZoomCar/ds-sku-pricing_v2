from src.entities.master_config import MasterConfig
from src.repositories.master_config_repository import MasterConfigRepository


class MasterConfigPublisher:
    def __init__(self):
        self.__repository = MasterConfigRepository()

    def create_master_config_record(self, city_id: int, car_type_id: int) -> None:
        record = MasterConfig()
        record.city_id = city_id
        record.car_type_id = car_type_id
        record.status = 1
        self.__repository.save_record(record)

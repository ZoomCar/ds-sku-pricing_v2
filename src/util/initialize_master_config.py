from src.models.pricing_model.master_config_publisher import MasterConfigPublisher
from src.models.supply_model.supply_model import SupplyModel
from src.util.logger_provider import attach_logger


@attach_logger
class MasterConfigService:
    def __init__(self):
        self.__supply_model = SupplyModel()
        self.__city_id_car_type_id_map = self.__supply_model.get_city_id_car_type_id_map()
        self.__publisher = MasterConfigPublisher()

    def worker(self):
        for city_id in self.__city_id_car_type_id_map:
            car_type_list = self.__city_id_car_type_id_map.get(city_id)
            for car_type_id in car_type_list:
                self.__publisher.create_master_config_record(city_id, car_type_id)
                self.logger.info(
                    f"successfully created master config record for city_id: {city_id}, car_type_id: {car_type_id}")


if __name__ == '__main__':
    MasterConfigService().worker()

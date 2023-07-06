from src.entities.master_config import MasterConfig
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class MasterConfigRepository:
    def __init__(self):
        self.__engine = PricingDBMySqlEngine()

    def get_record_by_id(self, id: int) -> MasterConfig:
        session = self.__engine.get_session()
        record = session.query(MasterConfig).get(id)
        session.close()
        return record

    def update_record(self, id: int, update_map: dict) -> None:
        session = self.__engine.get_session()
        session.query(MasterConfig) \
            .filter(MasterConfig.id == id) \
            .update(update_map)
        session.commit()
        session.close()

    def get_record_by_city_id(self, city_id: int) -> list:
        session = self.__engine.get_session()
        record_list = session.query(MasterConfig).filter(MasterConfig.city_id == city_id,
                                                         MasterConfig.status == 1).all()
        session.close()
        return record_list

    def get_record_by_city_id_and_car_type_id(self, city_id: int, car_type_id) -> MasterConfig:
        session = self.__engine.get_session()
        record = session.query(MasterConfig).filter(MasterConfig.city_id == city_id,
                                                    MasterConfig.car_type_id == car_type_id, ).first()
        session.close()
        return record

    def save_record(self, record: MasterConfig) -> None:
        session = self.__engine.get_session()
        session.add(record)
        session.commit()
        session.close()

    def save_records(self, record_list: list):
        session = self.__engine.get_session()
        session.bulk_save_objects(record_list)
        session.commit()
        session.close()

    def get_all_city_ids(self):
        session = self.__engine.get_session()
        active_city_list = [row.city_id for row in
                            session.query(MasterConfig.city_id).filter(MasterConfig.status == 1).distinct()]
        session.close()
        return active_city_list

    def get_all_records(self):
        session = self.__engine.get_session()
        record_list = session.query(MasterConfig).filter(MasterConfig.status == 1).all()
        session.close()
        return record_list

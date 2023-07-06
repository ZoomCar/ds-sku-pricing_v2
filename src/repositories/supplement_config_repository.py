from src.entities.supplement_config import SupplementConfig
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class SupplementConfigRepository:
    def __init__(self):
        self.__engine = PricingDBMySqlEngine()

    def get_record_by_id(self, id: int) -> SupplementConfig:
        session = self.__engine.get_session()
        record = session.query(SupplementConfig).get(id)
        session.close()
        return record

    def update_record(self, id: int, update_map: dict) -> None:
        session = self.__engine.get_session()
        session.query(SupplementConfig) \
            .filter(SupplementConfig.id == id) \
            .update(update_map)
        session.commit()
        session.close()

    def get_record_by_city_id(self, city_id: int) -> list:
        session = self.__engine.get_session()
        record_list = session.query(SupplementConfig).filter(SupplementConfig.city_id == city_id).all()
        session.close()
        return record_list

    def get_records_by_city_id_and_car_type_id(self, city_id: int, car_type_id: int) -> list:
        session = self.__engine.get_session()
        records = session.query(SupplementConfig).filter(SupplementConfig.city_id == city_id,
                                                         SupplementConfig.car_type_id == car_type_id).all()
        session.close()
        return records

    def save_record(self, record: SupplementConfig) -> None:
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
                            session.query(SupplementConfig.city_id).distinct()]
        session.close()
        return active_city_list

    def get_all_records(self):
        session = self.__engine.get_session()
        record_list = session.query(SupplementConfig).all()
        session.close()
        return record_list

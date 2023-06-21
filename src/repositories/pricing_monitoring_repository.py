from src.entities.dpm import DPM
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class PricingMonitoringRepository():
    def __init__(self):
        self.__engine = PricingDBMySqlEngine()

    def get_record_by_id(self, id: int) -> DPM:
        session = self.__engine.get_session()
        record = session.query(DPM).get(id)
        session.close()
        return record

    def get_all_records(self):
        session = self.__engine.get_session()
        records = session.query(DPM).all()
        session.close()
        return records

    def save_record(self, record: DPM) -> None:
        session = self.__engine.get_session()
        session.add(record)
        session.commit()
        session.close()

    def save_records(self, record_list: list):
        session = self.__engine.get_session()
        session.bulk_save_objects(record_list)
        session.commit()
        session.close()

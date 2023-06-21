from sqlalchemy.sql.expression import func
from src.entities.service_execution_record import ServiceExecutionRecord
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class ServiceExecutionTrackerRepository():
    def __init__(self):
        self.__engine = PricingDBMySqlEngine()

    def get_record_by_id(self, id: int) -> ServiceExecutionRecord:
        session = self.__engine.get_session()
        record = session.query(ServiceExecutionRecord).get(id)
        session.close()
        return record

    def get_max_id(self):
        session = self.__engine.get_session()
        record = session.query(func.max(ServiceExecutionRecord.id)).scalar()
        session.close()
        return record

    def update_record_by_id(self, id: int, update_map: dict) -> None:
        session = self.__engine.get_session()
        session.query(ServiceExecutionRecord) \
            .filter(ServiceExecutionRecord.id == id) \
            .update(update_map)
        session.commit()
        session.close()

    def save_record(self, record: ServiceExecutionRecord) -> int:
        session = self.__engine.get_session()
        session.add(record)
        session.commit()
        id = record.id
        session.close()
        return id

    def save_records(self, record_list: list):
        session = self.__engine.get_session()
        session.bulk_save_objects(record_list)
        session.commit()
        session.close()

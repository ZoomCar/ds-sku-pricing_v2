from sqlalchemy import func
from sqlalchemy.exc import OperationalError
from src.entities.dpm import DPM
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine
import os
import pandas as pd
from src.util.logger_provider import attach_logger

@attach_logger
class DpmRepository:
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

    def update_record_by_attributes(self, record: DPM):
        session = self.__engine.get_session()
        existing_record = session.query(DPM).filter(DPM.city_id == record.city_id,
                                                    DPM.car_id == record.car_id,
                                                    DPM.leadtime_start == record.leadtime_start,
                                                    DPM.leadtime_end == record.leadtime_end,
                                                    DPM.duration_start == record.duration_start,
                                                    DPM.duration_end == record.duration_end).first()
        if existing_record is None:
            print(f"no record found. saving it: {record}")
            self.save_record(record)
        else:
            # update record
            print(f"updating the record: {existing_record}")
            session.query(DPM).filter(DPM.id == existing_record.id).update({'dpm': record.dpm})
        session.commit()
        session.close()

    def update_record_by_id(self, id: int, update_map: dict) -> None:
        session = self.__engine.get_session()
        session.query(DPM) \
            .filter(DPM.id == id) \
            .update(update_map)
        session.commit()
        session.close()

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

    def delete_records_by_city(self, city_id: int):
        session = self.__engine.get_session()
        records = session.query(DPM).filter(DPM.city_id == city_id)
        records.delete(synchronize_session=False)
        session.commit()
        session.close()
        return records

    def delete_record_by_city_and_car_id(self, city_id: int, car_id: int):
        session = self.__engine.get_session()
        records = session.query(DPM).filter(DPM.city_id == city_id, DPM.car_id == car_id)
        records.delete(synchronize_session=False)
        session.commit()
        session.close()

    def delete_record_by_car_id(self, car_id_list):
        session = self.__engine.get_session()
        records = session.query(DPM).filter(DPM.car_id.in_(car_id_list))
        records.delete(synchronize_session=False)
        session.commit()
        session.close()

    def delete_record_by_car_id_lead_time_duration(self, car_id_list, lead_time, duration):
        session = self.__engine.get_session()
        records = session.query(DPM).filter(DPM.car_id.in_(car_id_list), DPM.le)
        records.delete(synchronize_session=False)
        session.commit()
        session.close()

    def get_max_run_id(self):
        try:
            session = self.__engine.get_session()
            max_run_id = session.query(func.max(DPM.run_id)).scalar()
            session.close()
        except OperationalError as e:
            if os.getenv("ENV") == "dev":
                print(f"could not fetch max run_id. This might be a vpn issue")
                return 1
            else:
                raise Exception(e)
        return max_run_id
    
    def get_sku_dpm_of_city(self, city_id):
        self.logger.info(f"started for city_id: {city_id}")
        query = f"""select city_id as cityId, 
                    car_id as carId,  
                    run_id as runId,
                    dpm as multiplier,
                    leadtime_start as leadTimeStart,
                    leadtime_end  as leadTimeEnd,
                    duration_start as durationStart,
                    duration_end as durationEnd
                    from zoomcar_analytics.sku_dpm  
                    where city_id={int(city_id)}
                    order by run_id, city_id, car_id
        """
        con_string = self.__engine.get_con_string()
        df = pd.read_sql_query(query, con=con_string)
        df[["cityId", "carId", "runId", "leadTimeStart", "leadTimeEnd", "durationStart", "durationEnd"]] = df[["cityId", "carId", "runId", "leadTimeStart", "leadTimeEnd", "durationStart", "durationEnd"]].astype('int')
        df["multiplier"] = df["multiplier"].astype('float')
        self.logger.info(f"finished for city_id: {city_id}")
        return df, df.runId.values
    
    def get_all_city_ids(self):
        self.logger.info(f"started get_all_city_ids")
        query = f"""select id as cityId from zoomcar.cities where active=1
        """
        con_string = self.__engine.get_con_string()
        df = pd.read_sql_query(query, con=con_string)
        df["cityId"] = df["cityId"].astype('int')
        city_ids =  [ int(city_id) for city_id in df.cityId.values]
        self.logger.info(f"active cities: {city_ids}")
        return city_ids

if __name__ == "__main__":
    repo = DpmRepository()
    print(repo.get_max_run_id())

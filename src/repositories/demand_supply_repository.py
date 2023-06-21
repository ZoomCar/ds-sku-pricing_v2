import os

import pandas as pd
from config import Config
from src.constants.constants import Constants
from src.entities.demand_supply import DemandSupply
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine
from src.util.time_util import get_current_timestamp_ist


class DemandSupplyRepository:
    def __init__(self):
        self.__engine = PricingDBMySqlEngine()
        self.__table_name = DemandSupply.__tablename__
    
    def get_max_run_id(self):
        con_string = self.__engine.get_con_string()
        df = pd.read_sql_query(f"SELECT MAX(run_id) as max_run_id FROM {self.__table_name}", con=con_string)
        if df.empty:
            return 0
        else:
            max_run_id = df.max_run_id.values[0]
        return max_run_id

    def save_demand_supply_data(self, df: pd.DataFrame):
        con_string = self.__engine.get_con_string()
        current_time = get_current_timestamp_ist()
        df.loc[:, "created_at"] = current_time
        df.loc[:, "updated_at"] = current_time
        df.loc[:, "created_by"] = "service"
        df.loc[:, "updated_by"] = "service"
        try:
            run_id = self.get_max_run_id() + 1
        except TypeError as e:
            run_id = 1
        df.loc[:, "run_id"] = run_id
        df.to_sql(con=con_string, name=self.__table_name, if_exists="append", index=False)

    def get_demand_supply_data(self, city_id, car_type_id, look_back_window=Constants.default_demand_supply_look_back):

        """
        look_back_window: number of run ids to look back from current max run id
        example: if max run id = 10 and look_back_window is 5 then
        take 10, 9, 8, 7, 6 run id data points
        """
        query = f"""
        SELECT * FROM {Config.pricing_db_name}.{self.__table_name}
        WHERE city_id = {city_id}
        AND car_type_id = 0
        AND (run_id > (SELECT MAX(run_id) AS max_run_id FROM {Config.pricing_db_name}.{self.__table_name} 
        WHERE city_id = {city_id}
        AND car_type_id = 0
        ) - {look_back_window}
        );"""
        con_string = self.__engine.get_con_string()
        df = pd.read_sql_query(query, con=con_string)
        return df


if __name__ == "__main__":
    demand_supply_repository = DemandSupplyRepository()
    _max_run_id = demand_supply_repository.get_max_run_id()
    print(_max_run_id)

from pprint import pprint

import pandas as pd

from config import Config
from src.constants.constants import Constants
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine
from src.repositories.dpm_repository import DpmRepository


class HistoricalDpmRepository:
    def __fetch_dynamic_pricing_features(self, record):
        dynamic_pricing_row = (record.id, record.run_id, record.city_id, record.car_id, record.leadtime_start,
                               record.leadtime_end, record.duration_start, record.duration_end, record.discount_status,
                               record.dpm, record.updated_at, record.updated_by, record.created_at, record.created_by)
        return dynamic_pricing_row

    def fetch_pricing_multipliers(self):
        dynamic_dpm_list = DpmRepository().get_all_records()
        column_list = ["id", "run_id", "city_id", "car_id", "leadtime_start", "leadtime_end",
                       "duration_start", "duration_end", "discount_status", "dpm", "updated_at",
                       "updated_by", "created_at", "created_by"]
        pricing_multipliers_df = pd.DataFrame(
            [self.__fetch_dynamic_pricing_features(record) for record in dynamic_dpm_list],
            columns=column_list)
        return pricing_multipliers_df

    def save_historical_dpms(self):
        historical_dpm_df = self.fetch_pricing_multipliers()
        db_engine = PricingDBMySqlEngine().get_engine()
        schema_name = Config.pricing_db_name
        historical_dpm_df.to_sql(Constants.get_historical_dpm_table_name(), db_engine,
                                 schema=schema_name, index=False, if_exists='append', chunksize=1000, method='multi')


if __name__ == '__main__':
    repo = HistoricalDpmRepository()
    response = repo.fetch_pricing_multipliers()
    pprint(response)
    repo.save_historical_dpms()

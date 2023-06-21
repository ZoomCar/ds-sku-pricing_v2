import pandas as pd

from src.constants.constants import Constants
from src.persistence.api.connection_pool_api import ConnectionPoolApi
from src.persistence.engines.dwh_postgres_engine import DWHPostgresEngine


class CargroupDemandRepository:

    @staticmethod
    def fetch_cargroup_demand_data(city_id, cargroup_id):
        utility_history_days = int(Constants.utility_history_days)
        dwh_connection = ConnectionPoolApi.get_dwh_db_con()
        cargroup_demand_query = f"""
                    select run_id, leadtime, duration, demand from
                    (select run_id, leadtime_end as leadtime, duration_end as duration, demand
                    from data_science.dynamic_pricing_demand
                    where city_id = {city_id} and cargroup_id = {cargroup_id}
                    and updated_at >= current_date-{utility_history_days}
                    and run_id > (select max(run_id) from data_science.dynamic_pricing_demand_searches_historical))
                    UNION 
                    (select run_id, leadtime_end as leadtime, duration_end as duration, demand
                    from data_science.dynamic_pricing_demand_searches_historical
                    where city_id = {city_id}
                    and updated_at >= current_date-{utility_history_days})
                    order by 1,2,3
                     """
        cargroup_demand_df = pd.read_sql_query(cargroup_demand_query, dwh_connection)
        cargroup_demand_df.drop_duplicates(inplace=True)
        return cargroup_demand_df

    @staticmethod
    def save_car_type_demand_data(demand_df):
        dwh_engine = DWHPostgresEngine().get_engine()
        demand_df.to_sql(Constants.cargroup_demand_table, dwh_engine,
                         schema=Constants.price_multipliers_schema, index=False, if_exists='append',
                         chunksize=1000, method='multi')


if __name__ == '__main__':
    cargroup_demand_repository = CargroupDemandRepository()

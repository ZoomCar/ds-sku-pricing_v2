import pandas as pd

from src.persistence.api.connection_pool_api import ConnectionPoolApi

class UtilDpmRepository:

    @staticmethod
    def get_pricing_delta(city_id):
        mysql_connection = ConnectionPoolApi.get_mysql_db_con_pool()
        pricing_delta_query = f'''select city_id, dt as date, delta
        							from zoomcar_analytics.pricing_data_input_delta
        							where city_id = {city_id}'''
        pricing_delta_df = pd.read_sql(pricing_delta_query, con=mysql_connection)
        return pricing_delta_df

    @staticmethod
    def get_price_multiplier(city_id):
        mysql_connection = ConnectionPoolApi.get_mysql_db_con_pool()
        pricing_delta_query = f'''select city_id, start_delta, end_delta, dpm
        							from zoomcar_analytics.pricing_multipliers
        							where city_id = {city_id}'''
        pricing_multipliers_df = pd.read_sql(pricing_delta_query, con=mysql_connection)
        return pricing_multipliers_df


if __name__ == '__main__':
    pricing_delta_df = UtilDpmRepository.get_pricing_delta(1)

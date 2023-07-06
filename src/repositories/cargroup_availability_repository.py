import pandas as pd

from src.constants.constants import Constants
from src.persistence.api.connection_pool_api import ConnectionPoolApi
from src.persistence.engines.dwh_postgres_engine import DWHPostgresEngine


# @todo rename this module
class CargroupAvailabilityRepository:

    @staticmethod
    def get_car_movements_hubs(city_id, car_type_id):
        # car_movements_query = f'''select distinct hub_id from INVENTORY.CAR_MOVEMENTS cm
        #                             where CURRENT_DATE() <= ends
        #                             and cargroup_id = {cargroup_id}
        #                             '''
        query = f"""SELECT DISTINCT movement.HUB_ID as hub_id FROM INVENTORY.CAR_MOVEMENTS as movement
                LEFT JOIN INVENTORY.CARGROUPS as cg 
                ON movement.CARGROUP_ID = cg.id 
                LEFT JOIN INVENTORY.CAR_TYPES as ct 
                ON cg.CAR_TYPE = ct.id 
                LEFT JOIN INVENTORY.LOCATIONS as loc
                ON movement.LOCATION_ID = loc.ID
                where CURRENT_DATE() <= movement.ends
                AND ct.id = {car_type_id} AND loc.CITY_ID = {city_id}; 
        """
        return pd.read_sql(query, ConnectionPoolApi.get_inventory_mysql_db_con_pool())

    @staticmethod
    def get_car_id_type_map_by_city(city_id):
        q = f"""SELECT cars.id as car_id, cars.LICENSE, cg.id as car_group_id, 
        cg.CAR_TYPE as car_type_id, ct.NAME as car_type, loc.ID as location_id, 
        loc.CITY_ID as city_id, loc.HUB_ID as hub_id
        FROM INVENTORY.CARS as cars 
        LEFT JOIN INVENTORY.CARGROUPS as cg 
        ON cars.CAR_GROUP_ID = cg.id 
        LEFT JOIN INVENTORY.CAR_TYPES as ct 
        ON cg.CAR_TYPE = ct.id 
        LEFT JOIN INVENTORY.LOCATIONS as loc
        ON cars.LOCATION_ID = loc.id 
        WHERE loc.CITY_ID = {city_id};"""
        return pd.read_sql(q, ConnectionPoolApi.get_inventory_mysql_db_con_pool())

    @staticmethod
    def fetch_cargroup_availability_data(city_id, cargroup_id):
        utility_history_days = int(Constants.utility_history_days)
        dwh_connection = ConnectionPoolApi.get_dwh_db_con()
        cargroup_availability_query = f"""
                            select run_id, leadtime_end as leadtime, duration_end as duration, 
                            case when supply = 0 then 1 else supply end as supply
                            from data_science.dynamic_pricing_supply
                            where city_id = {city_id} and cargroup_id = {cargroup_id}
                            and updated_at >= current_date-{utility_history_days}
                            and ((date_part(dow,updated_at) in (1,2,3,4,5) and date_part(dow, getdate()) in (1,2,3,4,5))
                            or (date_part(dow, updated_at) in (0,6) and date_part(dow, getdate()) in (0,6)))
                            order by 1,2 
                                    """
        cargroup_availability_df = pd.read_sql_query(cargroup_availability_query, dwh_connection)
        return cargroup_availability_df

    @staticmethod
    def save_cargroup_availability_data(cargroup_availability_df):
        dwh_engine = DWHPostgresEngine().get_engine()
        cargroup_availability_df.to_sql(Constants.supply_availability_table, dwh_engine,
                                        schema=Constants.price_multipliers_schema, index=False,
                                        if_exists='append', chunksize=1000, method='multi')


    @staticmethod
    def get_sku_dpm_mapping_of_city(city_id):
        query = f"""select city_id as cityId, 
                    car_id as carId,  
                    run_id as runId,
                    multiplier,
                    lead_time_start as leadTimeStart,
                    lead_time_end  as leadTimeEnd,
                    duration_start as durationStart,
                    duration_end as durationEnd
                    from pricing.SKU_DPM_MAPPING
                    where city_id={int(city_id)}
        """
        df = pd.read_sql(query, ConnectionPoolApi.get_inventory_mysql_db_con_pool())
        df[["cityId", "carId", "runId", "leadTimeStart", "leadTimeEnd", "durationStart", "durationEnd"]] = df[["cityId", "carId", "runId", "leadTimeStart", "leadTimeEnd", "durationStart", "durationEnd"]].astype('int')
        df["multiplier"] = df["multiplier"].astype('float')
        return df,df.runId.values
    
    @staticmethod
    def get_sku_dpm_mapping_run_ids_of_city(city_id):
        query = f"""select distinct run_id as runId
                    from pricing.SKU_DPM_MAPPING
                    where city_id={int(city_id)}
        """
        df = pd.read_sql(query, ConnectionPoolApi.get_inventory_mysql_db_con_pool())
        df[["runId"]] = df[["runId"]].astype('int')
        return df.runId.values
    
if __name__ == '__main__':
    car_movements_df = CargroupAvailabilityRepository.get_car_movements_hubs(19)

import pandas as pd

from src.constants.constants import Constants
from src.persistence.api.connection_pool_api import ConnectionPoolApi
from src.persistence.engines.dwh_postgres_engine import DWHPostgresEngine


class FeedbackRepository:

    @staticmethod
    def fetch_bookings_frequeny_data():
        booking_history_days = int(Constants.booking_history_days)
        dwh_connection = ConnectionPoolApi.get_dwh_db_con()
        bookings_query = f"""select dl.city_id, dc.cargroup_id, 
                    case when datediff(hours, fb.starts::timestamp, fb.ends::timestamp) < 24 then '0-24'
                        when datediff(hours, fb.starts::timestamp, fb.ends::timestamp) < 48 then '24-48'
                        when datediff(hours, fb.starts::timestamp, fb.ends::timestamp) >= 48 then '48+' end as duration,
                    case when datediff(hours, fb.created_at::timestamp, fb.starts::timestamp) < 24 then '0-24'
                        when datediff(hours, fb.created_at::timestamp, fb.starts::timestamp) < 48 then '24-48'
                        when datediff(hours, fb.created_at::timestamp, fb.starts::timestamp) >= 48 then '48+' end as leadtime,
                    {booking_history_days}*24/count(distinct booking_id) as avg_booking_frequency
                    from dwh.fact_booking fb
                    left join dwh.dim_location dl on dl.id = fb.id_location 
                    left join dwh.dim_car dc on dc.id = fb.id_car
                    where date(dateadd(m,330,created_at::timestamp)) >= current_date-{booking_history_days}
                    and booking_type != 'FREE_FLOAT'
                    and booking_medium in ('ANDROID', 'IOS')
                    and (lower(promo) not like 'commute%' or promo is null)
                    group by 1,2,3,4
                    order by 1,2,3,4
                     """
        bookings_df = pd.read_sql_query(bookings_query, dwh_connection)
        return bookings_df

    @staticmethod
    def fetch_bookings_data():
        booking_history_days = int(Constants.booking_history_days)
        inventory_connection = ConnectionPoolApi.get_inventory_mysql_db_con_pool()
        bookings_query = f"""select city_id, car_group_id as cargroup_id, 
                case when duration < 24 then '0-24'
                    when duration < 48 then '24-48'
                    when duration >= 48 then '48+' end as duration,
                case when leadtime < 24 then '0-24'
                    when leadtime < 48 then '24-48'
                    when leadtime >= 48 then '48+' end as leadtime,
                max(booking_created_at) as last_booking_created,
                TIMESTAMPDIFF(HOUR , max(booking_created_at), now()) as hours_since_last_booking,
                count(case when booking_created_at >= now() - interval 4 hour then booking_id end) as bookings
                from
                    (SELECT DISTINCT experiment_table.BOOKING_ID as booking_id, fragment_table.city_id, 
                    fragment_table.car_group_id, fragment_table.starts, fragment_table.ends, 
                    fragment_table.dpm, experiment_table.CREATED_AT as booking_created_at,
                    TIMESTAMPDIFF(HOUR , experiment_table.CREATED_AT, fragment_table.starts) as leadtime,
                    TIMESTAMPDIFF(HOUR, fragment_table.starts, fragment_table.ends) as duration
                    FROM
                    ((SELECT booking_id, CREATED_AT FROM pricing.BOOKING_EXPERIMENTS 
                            WHERE EXPERIMENT_NAME = 'CITY_LEVEL_DPM_EXP'
                            AND CREATED_AT >= CURRENT_DATE() - INTERVAL {booking_history_days} DAY) as experiment_table
                        LEFT JOIN (SELECT BOOKING_ID as booking_id, city_id, car_group_id, dpm, 
                                STATUS as status, starts, ends
                                FROM pricing.BOOKING_FRAGMENTS ) as fragment_table
                        ON experiment_table.booking_id = fragment_table.booking_id) 
                        WHERE fragment_table.status = 1 and city_id is not null) a
                group by 1,2,3,4
                order by 1,2,3,4;
                         """
        bookings_df = pd.read_sql_query(bookings_query, inventory_connection)
        return bookings_df

    @staticmethod
    def fetch_supply_data():
        dwh_connection = ConnectionPoolApi.get_dwh_db_con()
        supply_query = f"""select city_id, cargroup_id, 
                    case when duration_end < 24 then '0-24'
                        when duration_end < 48 then '24-48'
                        when duration_end >= 48 then '48+' end as duration,
                    case when leadtime_end < 24 then '0-24'
                        when leadtime_end < 48 then '24-48'
                        when leadtime_end >= 48 then '48+' end as leadtime,
                    max(case when run_id in (select max(run_id)-2 from data_science.dynamic_pricing_supply) 
                                                then supply end) as start_supply,
                    max(case when run_id in (select max(run_id) from data_science.dynamic_pricing_supply) 
                                                then supply end) as end_supply
                    from data_science.dynamic_pricing_supply
                    group by 1,2,3,4
                    order by 1,2,3,4
                    """
        supply_df = pd.read_sql_query(supply_query, dwh_connection)
        return supply_df

    @staticmethod
    def fetch_latest_dpm():
        dwh_connection = ConnectionPoolApi.get_dwh_db_con()
        supply_query = f"""select city_id, cargroup_id, 
                    case when duration_end < 24 then '0-24'
                        when duration_end < 48 then '24-48'
                        when duration_end >= 48 then '48+' end as duration,
                    case when leadtime_end < 24 then '0-24'
                        when leadtime_end < 48 then '24-48'
                        when leadtime_end >= 48 then '48+' end as leadtime,
                    avg(dpm) as dpm
                    from data_science.pricing_multiplier_dynamic_pricing_historical
                    where run_id in (select max(run_id)-1 from 
                                            data_science.pricing_multiplier_dynamic_pricing_historical)
                    group by 1,2,3,4
                    order by 1,2,3,4
                    """
        supply_df = pd.read_sql_query(supply_query, dwh_connection)
        return supply_df

    @staticmethod
    def save_feedback_data(demand_df):
        dwh_engine = DWHPostgresEngine().get_engine()
        demand_df.to_sql(Constants.feedback_table, dwh_engine,
                         schema=Constants.price_multipliers_schema, index=False, if_exists='append',
                         chunksize=1000, method='multi')


if __name__ == '__main__':
    cargroup_demand_repository = FeedbackRepository()

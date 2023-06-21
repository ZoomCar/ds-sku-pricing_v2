from src.constants.constants import Constants

def get_query():
    q = f"""select city_id, 0 as car_type_id,
        sum(case when date_diff('hour', search_time, starts) <=6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) <11 then 1 else 0 end) JIT_0_10,
        sum(case when date_diff('hour', search_time, starts) <=6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) between 11 and 24 then 1 else 0 end) JIT_10_24,
        sum(case when date_diff('hour', search_time, starts) <=6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) <=24 then 1 else 0 end) JIT_0_24,
        sum(case when date_diff('hour', search_time, starts) <=6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) between 25 and 48 then 1 else 0 end) JIT_24_48,
        sum(case when date_diff('hour', search_time, starts) <=6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) > 48 then 1 else 0 end) JIT_48_10000,
        sum(case when date_diff('hour', search_time, starts) >6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) <11 then 1 else 0 end) NJIT_0_10,
        sum(case when date_diff('hour', search_time, starts) >6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) between 11 and 24 then 1 else 0 end) NJIT_10_24,
        sum(case when date_diff('hour', search_time, starts) >6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) <=24 then 1 else 0 end) NJIT_0_24,
        sum(case when date_diff('hour', search_time, starts) >6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) between 25 and 48 then 1 else 0 end) NJIT_24_48,
        sum(case when date_diff('hour', search_time, starts) >6 and date_diff('hour', cast(starts as timestamp) ,cast(ends as timestamp)) > 48 then 1 else 0 end) NJIT_48_10000
    from
    (
            select * from (
        select 'Android' as Medium,
        search_time, starts, ends, city_id, cartype_id,
        ROW_NUMBER () over (partition by context_device_id, date(from_iso8601_timestamp(timestamp)) order by aar.receivedat desc) rnk
        from
            (select 
            case 
                when context.app.version >= '6.7.1' then properties.device_id 
                when context.app.version <= '6.7.0' then context.device.id
            end as context_device_id,
            from_iso8601_timestamp(timestamp) search_time,
            timestamp,
            receivedat,
            coalesce
                    (
                    cast(properties.starts_utc as timestamp),
                    date_add('minute', -330, cast(properties.start_date as timestamp))
                    ) starts,
            coalesce
                    (
                    cast(properties.ends_utc as timestamp),
                    date_add('minute', -330, cast(properties.end_date as timestamp))
                    ) ends,
            dl.city_id city_id,
            cgp.cartype cartype_id
            from click.android_checkout_response aar
            left join dwh.dim_location dl on dl.location_id = cast(properties.location_id as integer)
            left join zoomcar.cargroups cgp on cast(properties.car_group_id as integer) = cgp.id 
            where
            properties.category_id='mp_checkout_response'
            and date(aar.dt)  > date(date_add('day', ((-48-{Constants.past_hr})/24) -2 , current_timestamp AT TIME ZONE 'UTC'))
            and from_iso8601_timestamp(timestamp) > date_add('hour', -48-{Constants.past_hr}, current_timestamp AT TIME ZONE 'UTC')
            and from_iso8601_timestamp(timestamp) < date_add('hour', -{Constants.past_hr}, current_timestamp AT TIME ZONE 'UTC')
            and dl.city_id is not null
            and cgp.cartype is not null
        ) aar
        )
        where rnk=1
        UNION ALL
        (select *
        from
            (
                select 'IOS' as Medium,
                search_time, starts, ends, city_id, cartype_id,
                ROW_NUMBER () over (partition by context_device_id, date(from_iso8601_timestamp(timestamp)) order by aar.receivedat desc) rnk
                from
                    (select 
                    case 
                        when context.app.version >= '6.7.1' then properties.device_id 
                        when context.app.version <= '6.7.0' then context.device.id
                    end as context_device_id,
                    from_iso8601_timestamp(timestamp) search_time,
                    timestamp,
                    receivedat,
                    coalesce
                            (
                            cast(properties.starts_utc as timestamp),
                            date_add('minute', -330, cast(properties.start_date as timestamp))
                            ) starts,
                    coalesce
                            (
                            cast(properties.ends_utc as timestamp),
                            date_add('minute', -330, cast(properties.end_date as timestamp))
                            ) ends,
                    dl.city_id city_id,
                    cgp.cartype cartype_id
                    from click.ios_checkout_response aar
                    left join dwh.dim_location dl on dl.location_id = cast(properties.location_id as integer)
                    left join zoomcar.cargroups cgp on cast(properties.car_group_id as integer) = cgp.id 
                    where
                    properties.category_id='mp_checkout_response'
                    and date(aar.dt)  > date(date_add('day', ((-48-{Constants.past_hr})/24) -2 , current_timestamp AT TIME ZONE 'UTC'))
                    and from_iso8601_timestamp(timestamp) > date_add('hour', -48-{Constants.past_hr}, current_timestamp AT TIME ZONE 'UTC')
                    and from_iso8601_timestamp(timestamp) < date_add('hour', -{Constants.past_hr}, current_timestamp AT TIME ZONE 'UTC')
                    and dl.city_id is not null
                    and cgp.cartype is not null
                    ) aar)
        where rnk=1)
    )
    group by 1
    """
    return q

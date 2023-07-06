def get_query():
    query = f"""
    select * from (
select distinct city as city_id, car_type as car_type_id, 
case when m_0_24 = 1 and nb_0_24 = 1 then carid end av_inv_0_24,
case when m_24_48 = 1 and nb_24_48 = 1 then carid end av_inv_24_48,
case when m_48_72 = 1 and nb_48_72 = 1 then carid end av_inv_48_72
from 
(
select 
carid, 
car_type, 
city, 
max(movement_0_24) m_0_24,
max(no_block_0_24) nb_0_24,
count(no_booking_0_24) cnb_0_24, 
sum(no_booking_0_24) snb_0_24,
max(movement_24_48) m_24_48,
max(no_block_24_48) nb_24_48,
count(no_booking_24_48) cnb_24_48, 
sum(no_booking_24_48) snb_24_48,
max(movement_48_72) m_48_72,
max(no_block_48_72) nb_48_72,
count(no_booking_48_72) cnb_48_72, 
sum(no_booking_48_72) snb_48_72
from 
(
select 
ct.ID car_type, 
il.CITY_ID city, 
mov_block.cmid carid, 
mov_block.cm_starts, 
mov_block.cm_ends, 
mov_block.cbid, 
mov_block.cb_starts, 
mov_block.cb_ends, 
mov_block.movement_0_24, 
mov_block.no_block_0_24, 
mov_block.no_booking_0_24,
mov_block.movement_24_48,
mov_block.no_block_24_48,
mov_block.no_booking_24_48,
mov_block.movement_48_72,
mov_block.no_block_48_72,
mov_block.no_booking_48_72
from 
(
select *,
case when cm_starts < present_time and cm_ends > hour_24 then 1 else 0 end movement_0_24,
case when cb_ends < present_time or cb_starts > hour_24 or cb_ends is null or cb_starts is null then 1 else 0 end no_block_0_24,
case when bk.ENDS < present_time or bk.STARTS > hour_24 then 1 else 0 end no_booking_0_24,
case when cm_starts < hour_24 and cm_ends > hour_48 then 1 else 0 end movement_24_48,
case when cb_ends < hour_24 or cb_starts > hour_48 or cb_ends is null or cb_starts is null then 1 else 0 end no_block_24_48,
case when bk.ENDS < hour_24 or bk.STARTS > hour_48 then 1 else 0 end no_booking_24_48,
case when cm_starts < hour_48 and cm_ends > hour_72 then 1 else 0 end movement_48_72,
case when cb_ends < hour_48 or cb_starts > hour_72 or cb_ends is null or cb_starts is null then 1 else 0 end no_block_48_72,
case when bk.ENDS < hour_48 or bk.STARTS > hour_72 then 1 else 0 end no_booking_48_72,
bk.STARTS bk_starts, bk.ends bk_ends
from
(select carm.car_id cmid, starts cm_starts, ends cm_ends,status cm_status, MODIFIED_AT cm_modified_st ,
now() present_time,
date_add(now(), interval 24 hour) hour_24,
date_add(now(), interval 48 hour) hour_48,
date_add(now(), interval 72 hour) hour_72
FROM INVENTORY.CAR_MOVEMENTS carm
INNER JOIN (
    SELECT car_id, MAX(modified_at) AS max_start_date
    FROM INVENTORY.CAR_MOVEMENTS cm 
    GROUP BY car_id
) last_created_dates
ON carm.CAR_ID = last_created_dates.car_id AND carm.modified_at = last_created_dates.max_start_date 
where status =1
) alias_movement
left join 
(
select carb.car_id cbid, starts cb_starts, ends cb_ends,status cb_status, MODIFIED_AT cb_modified_st FROM INVENTORY.CAR_BLOCKS carb
INNER JOIN (
    SELECT car_id, MAX(modified_at) AS max_start_date
    FROM INVENTORY.CAR_BLOCKS b
    GROUP BY car_id
) last_created_dates
ON carb.CAR_ID = last_created_dates.car_id AND carb.modified_at = last_created_dates.max_start_date 
where status =1
) alias_block
on alias_movement.cmid = alias_block.cbid
left join INVENTORY.BOOKINGS bk on bk.CAR_ID = alias_movement.cmid and bk.CREATED_AT > date_add(now(), interval -15 day)
) mov_block
left join INVENTORY.CARS cars on cars.ID = mov_block.cmid 
left join INVENTORY.CARGROUPS cg on cg.id = cars.CAR_GROUP_ID 
left join INVENTORY.CAR_TYPES ct on ct.ID = cg.CAR_TYPE 
left join INVENTORY.LOCATIONS il on il.id = cars.LOCATION_ID 
-- where cmid = Test you Car ID here
) mov_block_available
group by carid,car_type,city
) av
) av_license
where av_inv_0_24 is not null
    """
    return query

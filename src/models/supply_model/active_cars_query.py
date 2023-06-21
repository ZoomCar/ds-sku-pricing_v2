def get_query():
    query = f"""
    select distinct cars.ID as car_id, 
        il.CITY_ID as city_id,  
        cg.CAR_TYPE as car_type_id
    FROM INVENTORY.CARS cars
    inner join INVENTORY.CARGROUPS cg on cg.id = cars.CAR_GROUP_ID 
    inner join INVENTORY.LOCATIONS il on il.id = cars.LOCATION_ID 
    where cars.STATUS = 1
    """
    return query

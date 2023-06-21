from src.repositories.cargroup_availability_repository import CargroupAvailabilityRepository
from src.repositories.cargroup_demand_repository import CargroupDemandRepository


class UtilityModel:
    def __init__(self, city_id, cargroup_id):
        self.__city_id = city_id
        self.__cargroup_id = cargroup_id

    def fetch_historical_demand_values(self):
        demand_df = CargroupDemandRepository.fetch_cargroup_demand_data(self.__city_id, self.__cargroup_id)
        demand_df[['run_id', 'demand']] = demand_df.groupby(['duration', 'leadtime']).transform(
            lambda x: x.fillna(x.mean()))
        return demand_df

    def fetch_historical_supply_values(self):
        supply_df = CargroupAvailabilityRepository.fetch_cargroup_availability_data(self.__city_id, self.__cargroup_id)
        supply_df[['run_id', 'supply']] = supply_df.groupby(['duration', 'leadtime']).transform(
            lambda x: x.fillna(x.mean()))
        return supply_df


if __name__ == '__main__':
    city_cargroup_df = UtilityModel(1, 19).fetch_historical_demand_values()

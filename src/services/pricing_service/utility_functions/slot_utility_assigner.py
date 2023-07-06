import math

from src.entities.pricing_engine_grid_cell import PricingEngineGridCell
from src.entities.supplement_config import SupplementConfig
from src.enums.decay_type_enum import DecayType
from src.enums.utility_type_enum import UtilityType
from src.models.demand_model.demand_model import DemandModel
from src.models.supply_model.supply_model import SupplyModel
from src.repositories.demand_supply_repository import DemandSupplyRepository
from src.util.logger_provider import attach_logger

@attach_logger
class SlotUtilityAssigner:
    def __init__(self, city_id, car_type_id, supply_model, demand_model):
        self.__city_id = city_id
        self.__car_type_id = car_type_id
        self.__supply_model = supply_model
        self.__demand_model = demand_model
        self.__demand_supply_repository = DemandSupplyRepository()
        self.__supply_data = self.__supply_model.get_supply(city_id, car_type_id)  # supply is assigned here
        self.__demand_data = self.__demand_model.get_demand(city_id, 0)  # demand is assigned here, fixing the car_type_id as 0
        self.logger.info(f"\n city_id: {city_id}, car_type_id: {car_type_id},\n demand: {self.__demand_data},\n supply: {self.__supply_data}")
    
    def get_demand_supply_key(self, lead_time, duration):
        # @todo this is ugly, need to find a better way to manage this
        # wise words: demand is on lead time + duration level supply is only on duration level
        if lead_time == 6:
            if duration == 10:
                demand_key = "demand_" + "jit_" + "0_10"
                supply_key = "supply_" + "0_10"
            elif duration == 24:
                demand_key = "demand_" + "jit_" + "10_24"
                supply_key = "supply_" + "10_24"
            elif duration == 48:
                demand_key = "demand_" + "jit_" + "24_48"
                supply_key = "supply_" + "24_48"
            else:
                demand_key = "demand_" + "jit_" + "48_10000"
                supply_key = "supply_" + "48_10000"
        else:
            if duration == 10:
                demand_key = "demand_" + "non_jit_" + "0_10"
                supply_key = "supply_" + "0_10"
            elif duration == 24:
                demand_key = "demand_" + "non_jit_" + "10_24"
                supply_key = "supply_" + "10_24"
            elif duration == 48:
                demand_key = "demand_" + "non_jit_" + "24_48"
                supply_key = "supply_" + "24_48"
            else:
                demand_key = "demand_" + "non_jit_" + "48_10000"
                supply_key = "supply_" + "48_10000"
        
        return demand_key, supply_key
    
    def get_mean_demand(self, grid_cell):
        lead_time = grid_cell.lead_time
        duration = grid_cell.duration

        demand_supply_df = self.__demand_supply_repository.get_demand_supply_data(self.__city_id, self.__car_type_id)
        demand_key, supply_key = self.get_demand_supply_key(lead_time, duration)
        
        if demand_supply_df is None or len(demand_supply_df)==0:
            self.logger.warning('demand_supply_sub_df is empty, so passing avg_demand as None')
            return None
        
        mean_demand = demand_supply_df[demand_key].median()
        self.logger.info(f"mean_demand: {mean_demand}")
        return mean_demand

    def _assign_supply_demand(self, grid_cell: PricingEngineGridCell):
        lead_time = grid_cell.lead_time
        duration = grid_cell.duration
        supply_value = self.__supply_data["lead_time"][lead_time]["duration"][duration]
        grid_cell.supply = supply_value
        demand_value = self.__demand_data["lead_time"][lead_time]["duration"][duration]
        grid_cell.demand = demand_value
        self.logger.info(f"lead_time: {lead_time}, duration: {duration}, demand:{grid_cell.demand}, supply: {grid_cell.supply}")
        return grid_cell

    def compute_utility(self, **kwargs):
        demand, supply = kwargs.get("demand"), kwargs.get("supply")
        if supply == 0:
            # self.logger.info(f"supply is being overwritten from 0 to 1")
            supply = 1  # this is a hack but pretty reasonable.

        if demand == 0:
            # self.logger.info(f"demand is being overwritten from 0 to 1")
            demand = 1  # hack but works. also this is rare that demand is 0

        # utility = supply / math.pow(demand, 1 / supply)
        # utility = round(utility, 2)
        utility = int(demand)
        return utility

    def _utility_assignment_handler(self, grid_cell: PricingEngineGridCell):
        """
        utility is defined as value for that slot. its mathematical definition is,
        available_supply/log(demand_delta) (deviation of demand from average value)
        higher the supply, higher the utility and lower the price.
        """

        self.logger.info(f"demand: {grid_cell.demand}, supply: {grid_cell.supply}")
        grid_cell.utility = self.compute_utility(demand=grid_cell.demand, supply=grid_cell.supply)

        grid_cell.mean_demand = self.get_mean_demand(grid_cell)

        self.logger.debug(
            f"city_id: {self.__city_id} car_type_id: {self.__car_type_id}, lead_time: {grid_cell.lead_time}, "
            f"duration: {grid_cell.duration}, supply: {grid_cell.supply}, demand: {grid_cell.demand}, "
            f"mean_demand: {grid_cell.mean_demand}")
        
        return grid_cell

    def assign_utility(self, grid):
        self.logger.info(f"starting utility computation")
        supply_assigned_grid = self._assign_supply_demand(grid)
        self.logger.info(f"successfully assigned demand and supply")
        utility_assigned_grid = self._utility_assignment_handler(supply_assigned_grid)
        self.logger.info(f"successfully assigned utility")
        return utility_assigned_grid


if __name__ == "__main__":
    slot_utility_assigner = SlotUtilityAssigner(1, 3, SupplyModel(), DemandModel())
    supplement_config = SupplementConfig()
    supplement_config.city_id = 1
    supplement_config.car_type_id = 3
    supplement_config.id = 1
    supplement_config.max_dpm = 1.2
    supplement_config.min_dpm = 0.8
    supplement_config.lead_time = 6
    supplement_config.duration = 24
    supplement_config.decay_type = DecayType.LINEAR_DECAY.value
    supplement_config.utility_type = UtilityType.SLOT_UTILITY.value

    slot_utility_assigner.assign_utility(supplement_config)
    # slot_utility_assigner.utility_stuff()

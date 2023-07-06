from src.entities.pricing_engine_grid_cell import PricingEngineGridCell
from src.enums.decay_type_enum import DecayType
from src.repositories.master_config_repository import MasterConfigRepository
from src.services.pricing_service.utility_price_xmer.lineary_xmer import LinearXmer
from src.util.logger_provider import attach_logger


@attach_logger
class SlotPriceAssigner:
    def __init__(self, city_id, car_type_id):
        self.__city_id = city_id
        self.__car_type_id = car_type_id
        self.__master_config_repository = MasterConfigRepository()

    def __assign_price(self, cell: PricingEngineGridCell):
        avg_dpm = (float(cell.max_dpm)+float(cell.min_dpm))/2.0

        if cell.mean_demand is None:
            cell.dpm = avg_dpm
            self.logger.warning(f'mean demand is None so returning avg dpm i.e {avg_dpm}')
        else:
            if cell.mean_demand==0:
                cell.mean_demand=1
            percent_change_in_demand_wrt_mean_demand = (float(cell.demand) - float(cell.mean_demand))/float(cell.mean_demand)
            cell.dpm = avg_dpm
            # avg_dpm*(1+percent_change_in_demand_wrt_mean_demand)
        
        if cell.dpm > cell.max_dpm:
            cell.dpm=cell.max_dpm
        elif cell.dpm < cell.min_dpm:
            cell.dpm=cell.min_dpm
        
            
        self.logger.debug(
            f"city_id: {self.__city_id} car_type_id: {self.__car_type_id}, lead_time: {cell.lead_time}, "
            f"duration: {cell.duration}, supply: {cell.supply}, demand: {cell.demand}, utility: {cell.utility}, "
            f"mean_demand: {cell.mean_demand}, min_utility: {cell.min_utility}, max_utility: {cell.max_utility}, "
            f"dpm: {cell.dpm}, min_dpm: {cell.min_dpm}, max_dpm: {cell.max_dpm}")
        return cell

    def assign_price(self, grid_cell):
        price_assigned_grid = self.__assign_price(grid_cell)
        audit_info = {"supply": grid_cell.supply, "demand": grid_cell.demand,
                      "utility": grid_cell.utility, "min_utility": grid_cell.min_utility,
                      "max_utility": grid_cell.max_utility, "city_id": self.__city_id,
                      "car_type_id": self.__car_type_id, "lead_time": grid_cell.lead_time,
                      "duration": grid_cell.duration, "min_dpm": grid_cell.min_dpm, "max_dpm": grid_cell.max_dpm,
                      "dpm": grid_cell.dpm}

        self.auditor.append(audit_info)
        return price_assigned_grid


if __name__ == "__main__":
    price_assigner = SlotPriceAssigner(6, 19)

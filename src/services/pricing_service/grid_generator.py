from src.constants.constants import Constants as c  # Constants is too verbose
from src.entities.pricing_engine_grid_cell import PricingEngineGridCell
from src.util.logger_provider import attach_logger


@attach_logger
class GridGenerator:
    def __init__(self, city_id, car_type_id):
        self.__city_id = city_id
        self.__car_type_id = car_type_id

    def _build_grid(self):
        # config_details = self._get_config_details()
        # in case city has custom lead time and duration bucket use that or take default values
        lead_time_buckets = c.default_lead_time_buckets
        duration_buckets = c.default_duration_buckets

        grid = []
        for i in lead_time_buckets:
            for j in duration_buckets:
                grid_cell = PricingEngineGridCell(i, j)
                grid.append(grid_cell)

        return grid

    def get_grid(self):
        grid = self._build_grid()
        self.logger.info(
            f"successfully generated grid for city_id: {self.__city_id}, car_type_id: {self.__car_type_id}")
        return grid


if __name__ == "__main__":
    obj = GridGenerator(1, 19)
    grid = obj.get_grid()
    print(grid)

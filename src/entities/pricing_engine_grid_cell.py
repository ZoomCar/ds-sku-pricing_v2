from src.entities.base_grid_cell import BaseGridCell


class PricingEngineGridCell(BaseGridCell):
    def __init__(self, lead_time, duration):
        self.__supply = None
        self.__demand = None
        self.__utility = None
        self.__dpm = None
        self.__min_utility = None
        self.__max_utility = None
        self.__mean_demand = None
        self.__min_dpm = None
        self.__max_dpm = None
        self.__decay_type = None
        super().__init__(lead_time, duration)

    @property
    def supply(self):
        return self.__supply

    @supply.setter
    def supply(self, value):
        self.__supply = value

    @property
    def demand(self):
        return self.__demand

    @demand.setter
    def demand(self, value):
        self.__demand = value

    @property
    def utility(self):
        return self.__utility

    @utility.setter
    def utility(self, value):
        self.__utility = value

    @property
    def dpm(self):
        return self.__dpm

    @dpm.setter
    def dpm(self, value):
        self.__dpm = value

    @property
    def min_utility(self):
        return self.__min_utility

    @min_utility.setter
    def min_utility(self, value):
        self.__min_utility = value

    @property
    def max_utility(self):
        return self.__max_utility

    @max_utility.setter
    def max_utility(self, value):
        self.__max_utility = value

    @property
    def mean_demand(self):
        return self.__mean_demand

    @mean_demand.setter
    def mean_demand(self, value):
        self.__mean_demand = value

    @property
    def min_dpm(self):
        return self.__min_dpm

    @min_dpm.setter
    def min_dpm(self, value):
        self.__min_dpm = value

    @property
    def max_dpm(self):
        return self.__max_dpm

    @max_dpm.setter
    def max_dpm(self, value):
        self.__max_dpm = value

    @property
    def decay_type(self):
        return self.__decay_type

    @decay_type.setter
    def decay_type(self, value):
        self.__decay_type = value

    def __repr__(self):
        return f'<lead_time: {self.lead_time} duration: {self.duration} min_dpm: {self.__min_dpm} max_dpm: {self.__max_dpm} ' \
               f'supply: {self.supply} demand: {self.demand} mean_demand: {self.mean_demand}, dpm: {self.dpm} ' \
               f'utility: {self.__utility}, min_utility: {self.__min_utility}, max_utility: {self.__max_utility} ' \
               f'decay_type: {self.__decay_type}>'

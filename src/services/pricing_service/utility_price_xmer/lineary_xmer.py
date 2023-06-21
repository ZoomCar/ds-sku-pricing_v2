class LinearXmer:
    def __init__(self, min_utility, max_dpm, max_utility, min_dpm):
        self.__min_utility = min_utility
        self.__max_utility = max_utility
        self.__max_dpm = max_dpm
        self.__min_dpm = min_dpm
        self.__slope = self.__assign_slope()
        self.__c = self.__assign_y_intercept()

    @property
    def slope(self):
        return self.__slope

    @property
    def y_intercept(self):
        return self.__c

    def __assign_slope(self):
        numerator = self.__min_dpm - self.__max_dpm  # y2 - y1 -> (dpm2 - dpm1)
        denominator = self.__max_utility - self.__min_utility  # (x2 - x1)
        if denominator != 0:
            slope = (numerator / denominator)
        elif denominator == 0:
            # possible case when utility is 0 through-out due to 0 supply, let slope be 0 which will result in max dpm
            slope = 0.0
        else:
            # suspicious case when supply is constant at a non-zero value throughout
            raise ValueError(f'value of utility is constant throughout at {self.__min_utility}. Review supply data.')
        return slope

    def __assign_y_intercept(self):
        # normalized
        try:
            y_intercept = self.__max_dpm - (self.__slope * self.__min_utility)
        except ZeroDivisionError as e:
            # possible case when utility is 0 through-out due to 0 supply then assign max price
            if self.__max_utility == self.__min_utility:
                y_intercept = self.__max_dpm
            else:
                # suspicious case when supply is constant at a non-zero value throughout
                raise ValueError(
                    f'value of utility is constant throughout at {self.__min_utility}. Review supply data.')

        return y_intercept

    def get_price(self, utility):
        # normalize utility with max value
        try:
            price = (self.__slope * utility) + self.__c
        except ZeroDivisionError as e:
            # possible case when utility is 0 through-out due to 0 supply then assign max price
            if self.__max_utility == self.__min_utility:
                price = self.__c  # max dpm
            else:
                # suspicious case when supply is constant at a non-zero value throughout
                raise ValueError(
                    f'value of utility is constant throughout at {self.__min_utility}. Review supply data.')

        price = round(price, 2)
        return price

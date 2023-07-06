class BaseGridCell:
    def __init__(self, lead_time: int, duration: int):
        self.__lead_time = lead_time
        self.__duration = duration

    @property
    def lead_time(self):
        return self.__lead_time

    @lead_time.setter
    def lead_time(self, value):
        self.__lead_time = value

    @property
    def duration(self):
        return self.__duration

    @duration.setter
    def duration(self, value):
        self.__duration = value


if __name__ == "__main__":
    # sample API
    grid = BaseGridCell(1, 100)
    # get value
    print(grid.lead_time, grid.duration)
    # set value
    grid.lead_time = 2
    grid.duration = 24
    print(grid.lead_time, grid.duration)

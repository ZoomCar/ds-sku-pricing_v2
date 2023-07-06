import os

from config import Config


class PathNavigator:
    @staticmethod
    def base_path():
        # fetch this from config while pushing to prod
        return Config.base_path

    @staticmethod
    def data_folder_name():
        return "data"

    @staticmethod
    def config_path():
        # fetch this from env vars
        return os.path.join(PathNavigator.base_path(), "config", "pricing_dev_config.ini")

    @staticmethod
    def data_folder_path():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name())

    @staticmethod
    def demand_data_for_conversion():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "demand_data")

    @staticmethod
    def bookings_data_for_conversion():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "bookings_data")

    @staticmethod
    def conversion_data_path():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "conversion_data")

    @staticmethod
    def peak_days_2019_data():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "peak_days_data")

    @staticmethod
    def holidays_list_data():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "holidays_data")

    @staticmethod
    def util_data_path():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "util_data")

    @staticmethod
    def demand_expected_price_data_path():
        return os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "demand_expected_price")

    @staticmethod
    def predicted_demand_data_path():
        return os.path.join(PathNavigator.base_path(), "demand_prediction", "predictions")

    @staticmethod
    def price_engine_booking_data():
        path = os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "price_engine_booking_data")
        return path

    @staticmethod
    def price_data():
        path = os.path.join(PathNavigator.base_path(), PathNavigator.data_folder_name(), "price_data")
        return path

    @staticmethod
    def log_folder_path():
        return os.path.join(PathNavigator.base_path(), "log")

    @staticmethod
    def conversion_function_config_file_path():
        base_path = os.path.join(PathNavigator.base_path(), "data", "conversion_function_config")
        file_name = "config.csv"
        file_path = os.path.join(base_path, file_name)
        return file_path

    @staticmethod
    def get_conversion_function_config_folder():
        return os.path.join(PathNavigator.get_data_storage_folder_name(), 'conversion_function_config')

    @staticmethod
    def get_monitoring_reports_folder():
        return os.path.join(PathNavigator.base_path(), "monitoring", "reports")

    @staticmethod
    def get_data_folder():
        return os.path.join(PathNavigator.base_path(), 'data')

    @staticmethod
    def get_supply_model_data_folder():
        return os.path.join(PathNavigator.get_data_folder(), 'supply_model')

    @staticmethod
    def get_car_availability_grid_folder():
        return os.path.join(PathNavigator.get_supply_model_data_folder(), 'car_availability_grid')

    @staticmethod
    def get_cargroup_availability_grid_folder():
        return os.path.join(PathNavigator.get_supply_model_data_folder(), 'cargroup_availability_grid')

    @staticmethod
    def get_city_availability_folder():
        return os.path.join(PathNavigator.get_supply_model_data_folder(), 'city_availability')

    @staticmethod
    def get_hub_availability_folder():
        return os.path.join(PathNavigator.get_supply_model_data_folder(), 'hub_availability')

    @staticmethod
    def get_latest_inventory_snapshots_folder():
        return os.path.join(PathNavigator.get_supply_model_data_folder(), 'latest_inventory_snapshot')

    @staticmethod
    def get_historical_inventory_snapshots_folder():
        return os.path.join(PathNavigator.get_supply_model_data_folder(), 'historical_inventory_snapshot')

    @staticmethod
    def get_demand_grid_folder():
        return os.path.join(PathNavigator.get_data_folder(), 'demand_model')


if __name__ == "__main__":
    print(PathNavigator.base_path())

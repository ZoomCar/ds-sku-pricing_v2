import json


class DataWriter:
    """used to write data on disk Not for remote (db/cloud) data writing"""

    def __init__(self):
        pass

    @staticmethod
    def write_csv_data(file_path, df):
        """
        Args:
            file_path:
            df:
        """
        try:
            df.to_csv(file_path, index=False)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def write_json_data(file_path, data_dict):
        """
        Args:
            file_path:
            data_dict:
        """
        try:
            with open(file_path, 'w') as f:
                json.dump(data_dict, f)
        except Exception as e:
            raise Exception(str(e))


import json
import os
import pandas as pd


class DataReader:
    """reads data from disk Not for remote (db/cloud) data fetching"""

    def __init__(self):
        pass

    @staticmethod
    def load_json_data(file_path):
        """
        Args:
            file_path:
        """
        if not os.path.isfile(file_path):
            return None
        with open(file_path, "r") as f:
            json_data = json.load(f)
            return json_data

    @staticmethod
    def read_csv_data(file_path):
        """
        Args:
            file_path:
        """
        if not os.path.isfile(file_path):
            return None

        try:
            df = pd.read_csv(file_path)
            return df
        except (OSError, IOError) as e:
            raise Exception(e)


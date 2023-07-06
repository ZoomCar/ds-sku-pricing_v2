import os
import shutil

from src.util.path_navigator import PathNavigator

"""
dedicated to all file/folder/naming related operations. 
"""


def check_and_create_folder(dir_path):
    """
    Args:
        dir_path:
    """
    if (os.path.exists(dir_path)):
        pass
    else:
        print("creating: " + dir_path)
        try:
            os.makedirs(dir_path)
        except Exception as e:
            raise Exception("error while creating directory: <{0}>. Error stack: {1}".format(dir_path, str(e)))


def remove_temporary_folders():
    delete_folders = [PathNavigator.get_hub_availability_folder(),
                      PathNavigator.get_latest_inventory_snapshots_folder(),
                      PathNavigator.get_demand_grid_folder()]
    for folder_path in delete_folders:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

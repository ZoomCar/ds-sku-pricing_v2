import pandas as pd
from src.models.demand_model.demand_model import DemandModel
from src.models.supply_model.supply_model import SupplyModel
from src.repositories.demand_supply_repository import DemandSupplyRepository
from src.constants.constants import Constants

demand_model = DemandModel()
demand_df = demand_model.get_demand_data_frame()
demand_df.fillna(0, inplace=True)
column_rename_map = {
                    "JIT_0_24": "demand_jit_0_24",
                    "JIT_0_10": "demand_jit_0_10", 
                    "JIT_10_24": "demand_jit_10_24",
                    "JIT_24_48": "demand_jit_24_48",
                    "JIT_48_10000": "demand_jit_48_10000", 
                    "NJIT_0_24": "demand_non_jit_0_24",
                    "NJIT_0_10": "demand_non_jit_0_10",
                    "NJIT_10_24": "demand_non_jit_10_24", 
                    "NJIT_24_48": "demand_non_jit_24_48", 
                    "NJIT_48_10000": "demand_non_jit_48_10000"
                    }
demand_df.rename(columns=column_rename_map, inplace=True)
try:
    DemandSupplyRepository().save_demand_supply_data(demand_df)
    print(f"successfully saved demand supply data into DB.")
except Exception as e:
    # raise alert
    print(f"failed to save demand supply data in DB. Error: {e}")

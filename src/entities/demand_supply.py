import os

from sqlalchemy import Column, Integer, SmallInteger, UniqueConstraint

from src.entities.base_entity import Base, BaseEntity
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine

class DemandSupply(Base, BaseEntity):
    __tablename__ = 'sku_supply_demand_v1'
    
    __table_args__ = (
        UniqueConstraint("run_id", "city_id", "car_type_id"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, nullable=False)
    city_id = Column(SmallInteger, nullable=False)
    car_type_id = Column(SmallInteger, nullable=False)
    demand_jit_0_10 = Column(Integer, nullable=False)
    demand_jit_10_24 = Column(Integer, nullable=False)
    demand_jit_0_24 = Column(Integer, nullable=False)
    demand_jit_24_48 = Column(Integer, nullable=False)
    demand_jit_48_10000 = Column(Integer, nullable=False)
    demand_non_jit_0_10 = Column(Integer, nullable=False)
    demand_non_jit_10_24 = Column(Integer, nullable=False)
    demand_non_jit_0_24 = Column(Integer, nullable=False)
    demand_non_jit_24_48 = Column(Integer, nullable=False)
    demand_non_jit_48_10000 = Column(Integer, nullable=False)
    
    def __repr__(self):
        return f"<id: {self.id}, run_id:{self.run_id}, city_id: {self.city_id}, car_type_id: {self.car_type_id}" \
               f">"


if __name__ == "__main__":
    engine_creator = PricingDBMySqlEngine()
    engine = engine_creator.get_engine()
    Base.metadata.create_all(engine)

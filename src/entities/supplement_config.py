import os

from sqlalchemy import Column, Numeric, SmallInteger, UniqueConstraint, String

from src.entities.base_entity import Base, BaseEntity
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class SupplementConfig(Base, BaseEntity):
    __tablename__ = 'sku_pricing_supplement_config_v1'
    __table_args__ = (
        UniqueConstraint("city_id", "car_type_id", "lead_time", "duration"),
    )
    city_id = Column(SmallInteger, nullable=False)
    car_type_id = Column(SmallInteger, nullable=False)
    lead_time = Column(SmallInteger, nullable=False)
    duration = Column(SmallInteger, nullable=False)
    min_dpm = Column(Numeric(scale=2, precision=4), nullable=False)
    max_dpm = Column(Numeric(scale=2, precision=4), nullable=False)
    utility_type = Column(String(100), nullable=False)
    decay_type = Column(String(100), nullable=False)

    def __repr__(self):
        return f"<id: {self.id}, city_id: {self.city_id} car_group_id: {self.car_type_id}, lead_time: {self.lead_time}," \
               f"duration: {self.duration},  min_dpm: {self.min_dpm}, max_dpm: {self.max_dpm}, " \
               f"utility_type: {self.utility_type} decay_type: {self.decay_type}>"


if __name__ == "__main__":
    engine_creator = PricingDBMySqlEngine()
    engine = engine_creator.get_engine()
    Base.metadata.create_all(engine)

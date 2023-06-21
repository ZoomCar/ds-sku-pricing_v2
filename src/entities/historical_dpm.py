import os

from sqlalchemy import Column, Integer, SmallInteger, Numeric, UniqueConstraint, String

from src.entities.base_entity import Base, BaseEntity
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class HistoricalDPM(Base, BaseEntity):
    __tablename__ = 'sku_historical_dpm_v1'

    __table_args__ = (
        UniqueConstraint("run_id", "city_id", "car_id", "leadtime_start", "leadtime_end", "duration_start",
                         "duration_end"),
    )
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, nullable=False)
    city_id = Column(SmallInteger, nullable=False)
    car_id = Column(Integer, nullable=False)
    leadtime_start = Column(SmallInteger, nullable=False)
    leadtime_end = Column(SmallInteger, nullable=False)
    duration_start = Column(SmallInteger, nullable=False)
    duration_end = Column(SmallInteger, nullable=False)
    discount_status = Column(String(255), nullable=False)
    dpm = Column(Numeric(scale=2, precision=4), nullable=False)

    def __repr__(self):
        return f"<id: {self.id}, run_id: {self.run_id}, " \
               f"city_id: {self.city_id}, " \
               f"car_id: {self.car_id}, leadtime_start: {self.leadtime_start}, " \
               f"leadtime_end: {self.leadtime_end}, duration_start: {self.duration_start}, " \
               f"duration_end: {self.duration_end}, dpm: {self.dpm}"


if __name__ == "__main__":
    engine_creator = PricingDBMySqlEngine()
    engine = engine_creator.get_engine()
    Base.metadata.create_all(engine)

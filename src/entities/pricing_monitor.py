from sqlalchemy import Column, DateTime, Numeric, String, func, SmallInteger, UniqueConstraint, Integer

from src.entities.base_entity import Base
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class PricingMonitor(Base):
    __tablename__ = 'dynamic_pricing_service_monitor'
    __table_args__ = (
        UniqueConstraint("run_id", "city_id", "car_group_id", "leadtime_start", "leadtime_end", "duration_start",
                         "duration_end"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, nullable=False)
    city_id = Column(SmallInteger, nullable=False)
    car_group_id = Column(SmallInteger, nullable=False)
    leadtime_start = Column(SmallInteger, nullable=False)
    leadtime_end = Column(SmallInteger, nullable=False)
    duration_start = Column(SmallInteger, nullable=False)
    duration_end = Column(SmallInteger, nullable=False)
    generated_dpm = Column(Numeric(scale=2, precision=4))
    changed_dpm = Column(Numeric(scale=2, precision=4), nullable=False)
    status = Column(String(100), nullable=False)
    created_on = Column(DateTime(timezone=True), default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<id: {self.id}, run_id: {self.run_id}, city_id: {self.city_id}, car_group_id: {self.car_group_id}," \
               f"leadtime_start: {self.leadtime_start}, leadtime_end: {self.leadtime_end}, " \
               f"duration_start: {self.duration_start}, duration_end: {self.duration_end}, " \
               f"generated_dpm: {self.generated_dpm}, changed_dpm: {self.changed_dpm}, " \
               f"status: {self.status}, created_on: {self.created_on}>"


if __name__ == "__main__":
    engine_creator = PricingDBMySqlEngine()
    engine = engine_creator.get_engine()
    Base.metadata.create_all(engine)

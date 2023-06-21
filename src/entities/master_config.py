import os
from sqlalchemy import Column, Boolean, SmallInteger, UniqueConstraint
from src.entities.base_entity import Base, BaseEntity
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine

class MasterConfig(Base, BaseEntity):
    __tablename__ = 'sku_pricing_master_config'
    
    __table_args__ = (
        UniqueConstraint("city_id", "car_type_id"),
    )
    city_id = Column(SmallInteger, nullable=False)
    car_type_id = Column(SmallInteger, nullable=False)
    status = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<id: {self.id}, city_id: {self.city_id} car_type_id: {self.car_type_id}>"


if __name__ == "__main__":
    engine_creator = PricingDBMySqlEngine()
    engine = engine_creator.get_engine()
    Base.metadata.create_all(engine)

from sqlalchemy import Column, DateTime, func, Integer, String

from src.entities.base_entity import Base
from src.persistence.engines.pricing_db_mysql_engine import PricingDBMySqlEngine


class ServiceExecutionRecord(Base):
    __tablename__ = 'pricing_service_tracker'

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False)
    start_time = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=True)  # a new job entry will have NULL end time
    created_on = Column(DateTime(timezone=True), default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<id: {self.id}, version: {self.version} status: {self.status}," \
               f"start_time: {self.start_time}> end_time: {self.end_time}"


if __name__ == "__main__":
    engine_creator = PricingDBMySqlEngine()
    engine = engine_creator.get_engine()
    Base.metadata.create_all(engine)

from sqlalchemy import Integer, Column, func, DateTime, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseEntity:
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    created_by = Column(String(255), default="service")
    updated_by = Column(String(255), default="service")

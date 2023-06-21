from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import Config
from src.persistence.engines.base_engine import BaseEngine


class PricingDBMySqlEngine(BaseEngine):
    def get_con_string(self):
        con_string = 'mysql+pymysql://' + Config.pricing_db_user + ':' + \
                     Config.pricing_db_pwd + '@' + Config.pricing_db_host + \
                     ':' + str(Config.pricing_db_port) + '/' + \
                     Config.pricing_db_name

        return con_string

    def get_engine(self):
        con_string = self.get_con_string()
        engine = create_engine(con_string, echo=False)
        return engine

    def get_session(self):
        Session = sessionmaker()
        Session.configure(bind=self.get_engine())
        session = Session()
        return session

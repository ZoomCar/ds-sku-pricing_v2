from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import Config
from src.persistence.engines.base_engine import BaseEngine


class DWHPostgresEngine(BaseEngine):
    def get_engine(self):
        con_string = 'postgresql://' + Config.dwh_db_user + ':' + \
                     Config.dwh_db_pwd + '@' + Config.dwh_db_host + ':' + str(Config.dwh_db_port) + '/' + \
                     Config.dwh_db_name
        engine = create_engine(con_string, echo=False)
        return engine

    def get_session(self):
        Session = sessionmaker()
        Session.configure(bind=self.get_engine())
        session = Session()
        return session

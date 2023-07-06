import psycopg2
import pymysql
from DBUtils.PooledDB import PooledDB
from pymongo import MongoClient

from config import Config

# keeping globals for singletons
redshift_db_pool = None
mysql_db_pool = None
inventory_mysql_db_pool = None
mongo_client = None


class ConnectionPoolApi:
    # this is zoomcar mysql slave db
    @staticmethod
    def get_mysql_db_con_pool():
        global mysql_db_pool  # singleton
        if mysql_db_pool is None:
            mysql_db_pool = PooledDB(creator=pymysql, host=Config.zoomcar_prod_db_host,
                                     user=Config.zoomcar_prod_db_user,
                                     password=Config.zoomcar_prod_db_pwd, database=Config.zoomcar_analytics_prod_db,
                                     autocommit=True,
                                     blocking=True, maxconnections=int(Config.zoomcar_prod_db_max_con))
        db = mysql_db_pool.connection(shareable=False)
        return db

    @staticmethod
    def get_dwh_db_con():
        con = psycopg2.connect(database=Config.dwh_db_name, host=Config.dwh_db_host, port=int(Config.dwh_db_port),
                               user=Config.dwh_db_user, password=Config.dwh_db_pwd)

        return con

    @staticmethod
    def get_inventory_mongo_db_con():
        global mongo_client
        if mongo_client is None:
            mongo_client = MongoClient(Config.prod_mongodb_host, username=Config.prod_mongodb_user,
                                       password=Config.prod_mongodb_pwd,
                                       authSource=Config.prod_mongodb_inventory_db,
                                       maxPoolSize=Config.prod_mongodb_max_connection)
        db = mongo_client[Config.prod_mongodb_inventory_db]
        return db

    @staticmethod
    def get_inventory_mysql_db_con_pool():
        global inventory_mysql_db_pool  # singleton
        if inventory_mysql_db_pool is None:
            inventory_mysql_db_pool = PooledDB(creator=pymysql, host=Config.prod_zoomcar_inventory_host,
                                               user=Config.prod_zoomcar_inventory_user,
                                               password=Config.prod_zoomcar_inventory_pwd,
                                               database=Config.prod_zoomcar_inventory_db, autocommit=True,
                                               blocking=True, maxconnections=int(Config.prod_zoomcar_inventory_max_con))
        db = inventory_mysql_db_pool.connection(shareable=False)
        return db

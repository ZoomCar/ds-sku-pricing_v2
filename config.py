import os

from dotenv import load_dotenv

load_dotenv(verbose=True)


class Config:
    env = os.getenv("ENV")
    base_path = os.getenv("base_path")
    zoomcar_prod_db_user = os.getenv("zoomcar_prod_db_user")
    zoomcar_prod_db_pwd = os.getenv("zoomcar_prod_db_pwd")
    zoomcar_prod_db_host = os.getenv("zoomcar_prod_db_host")
    zoomcar_prod_db_port = os.getenv("zoomcar_prod_db_port")
    zoomcar_prod_db_max_con = os.getenv("zoomcar_prod_db_max_con")
    zoomcar_analytics_prod_db = os.getenv("zoomcar_analytics_prod_db")
    dwh_db_user = os.getenv("dwh_db_user")
    dwh_db_pwd = os.getenv("dwh_db_pwd")
    dwh_db_host = os.getenv("dwh_db_host")
    dwh_db_port = os.getenv("dwh_db_port")
    dwh_db_max_con = os.getenv("dwh_db_max_con")
    dwh_db_name = os.getenv("dwh_db_name")
    prod_zoomcar_inventory_host = os.getenv("prod_zoomcar_inventory_host")
    prod_zoomcar_inventory_user = os.getenv("prod_zoomcar_inventory_user")
    prod_zoomcar_inventory_db = os.getenv("prod_zoomcar_inventory_db")
    prod_zoomcar_inventory_pwd = os.getenv("prod_zoomcar_inventory_pwd")
    prod_zoomcar_inventory_port = os.getenv("prod_zoomcar_inventory_port")
    prod_zoomcar_inventory_max_con = os.getenv("prod_zoomcar_inventory_max_con")
    pricing_db_user = os.getenv("pricing_db_user")
    pricing_db_pwd = os.getenv("pricing_db_pwd")
    pricing_db_host = os.getenv("pricing_db_host")
    pricing_db_port = os.getenv("pricing_db_port")
    pricing_db_max_con = os.getenv("pricing_db_max_con")
    pricing_db_name = os.getenv("pricing_db_name")
    aws_access_key_id = os.getenv("aws_access_key_id")
    aws_secret_access_key = os.getenv("aws_secret_access_key")
    region_name = os.getenv("region_name")
    reservoir_db_name = os.getenv("reservoir_db_name")
    s3_bucket_name = os.getenv("s3_bucket_name")
    s3_folder_name = os.getenv("s3_folder_name")
    discount_csv_path = os.getenv("discount_csv_path")
    api_email = os.getenv("api_email")
    api_token = os.getenv("api_token")
    sku_dpm_mapping_bulk_add_api_url = os.getenv("sku_dpm_mapping_bulk_add_api_url")
    sku_dpm_mapping_delete_api_url = os.getenv("sku_dpm_mapping_delete_api_url")


if __name__ == "__main__":
    print(Config.env)
    print(Config.base_path)
    print(Config.zoomcar_prod_db_host)

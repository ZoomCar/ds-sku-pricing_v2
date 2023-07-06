import io

import boto3
import pandas as pd

from config import Config
from src.models.demand_model.demand_query import get_query
from src.util.logger_provider import attach_logger


@attach_logger
class AthenaApi:
    def __init__(self, Athena_database, S3_result_folder, S3_bucket, S3_region_name):
        # if Config.env == "dev":
        #     self.resource = boto3.resource('s3', region_name=Config.region_name,
        #                                    aws_access_key_id=Config.aws_access_key_id,
        #                                    aws_secret_access_key=Config.aws_secret_access_key)
        #     self.client = boto3.client('athena', region_name=Config.region_name,
        #                                aws_access_key_id=Config.aws_access_key_id,
        #                                aws_secret_access_key=Config.aws_secret_access_key)
        # else:
        #     # use IAM role is in place
        #     self.resource = boto3.resource('s3', region_name=Config.region_name)
        #     self.client = boto3.client('athena', region_name=Config.region_name)

        self.resource = boto3.resource('s3', region_name=Config.region_name,
                                        aws_access_key_id=Config.aws_access_key_id,
                                        aws_secret_access_key=Config.aws_secret_access_key)
        self.client = boto3.client('athena', region_name=Config.region_name,
                                    aws_access_key_id=Config.aws_access_key_id,
                                    aws_secret_access_key=Config.aws_secret_access_key)
                        
        self.database = Athena_database
        self.folder = S3_result_folder
        self.bucket = S3_bucket
        self.s3_output = 's3://' + self.bucket + '/' + self.folder
        self.region_name = S3_region_name

    def load_conf(self, q):  # to run query over Athena
        try:
            response = self.client.start_query_execution(
                QueryString=q,
                QueryExecutionContext={
                    'Database': self.database
                },
                ResultConfiguration={
                    'OutputLocation': self.s3_output,
                }
            )
            self.filename = response['QueryExecutionId']
            self.logger.info('Execution ID: ' + response['QueryExecutionId'])
            return response
        except Exception as e:
            self.logger.info("Error Stack :{}".format(e))
            raise Exception(e)

    def run_query(self, athena_query):  # call load_conf method,to run query over Athena and check its status,
        # whether it is running, queued or failed
        queries = [athena_query]
        for q in queries:
            res = self.load_conf(q)
        try:
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = \
                    self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution'][
                        'Status'][
                        'State']
                self.logger.info(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string failed or was cancelled')
            df = self.obtain_data()
            return df
        except Exception as e:
            raise (e)

    def obtain_data(self):
        try:
            response = self.resource.Bucket(self.bucket).Object(key=self.folder + self.filename + '.csv').get()
            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')
        except Exception as e:
            raise (e)


if __name__ == "__main__":
    athena_api = AthenaApi(Config.reservoir_db_name, Config.s3_folder_name, Config.s3_bucket_name,
                           Config.region_name)

    query = get_query()
    df = athena_api.run_query(query)
    output_file = "demand_data.csv"
    df.to_csv(output_file, index=False)
    print(df)

import boto3

s3_connection = None


class S3Api:
    def __init__(self):
        pass

    @staticmethod
    def get_s3_client():
        global s3_connection
        if s3_connection is None:
            s3_connection = boto3.client('s3')
        return s3_connection

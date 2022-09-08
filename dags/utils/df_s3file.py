import pandas as pd
import geopandas as gpd
import boto3
import io

class DataFrameS3File:

    def __init__(self) -> None:
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        super().__init__()
    

    def get_full_key(self, bucket: str, name: str, prefix: str="") -> list:
        bucket = self.s3_resource.Bucket(bucket)
        keys = [obj.key for obj in bucket.objects.filter(Prefix=prefix)
                if name in obj.key]

        if keys:
            # return f's3://{bucket}/{keys[0]}'
            return keys[0]

        return None


    def get_df(self, bucket: str, name: str, prefix: str="") -> pd.DataFrame:
        key = self.get_full_key(bucket, name, prefix)
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    

    def get_gpd(self, bucket: str, name: str, prefix: str="") -> gpd.GeoDataFrame:
        key = self.get_full_key(bucket, name, prefix)
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return gpd.read_file(io.BytesIO(obj['Body'].read()))

    

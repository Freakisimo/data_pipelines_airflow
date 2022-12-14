from airflow.decorators import dag, task
from datetime import datetime, timedelta

from utils.df_s3file import DataFrameS3File

default_args = {
    'owner': 'Freakisimo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='process_c5_data',
    default_args=default_args,
    start_date=datetime(2022,9,1),
    schedule_interval='@monthly'
)
def process_gtfs_data():

    dfs3f = DataFrameS3File()
    bucket = 'datos-cdmx'

    @task
    def process_data():
        incidents = dfs3f.get_df(bucket, 'inviales_completa')

        print(incidents.columns)
        return None


    process_data()

process_gtfs_dag = process_gtfs_data()
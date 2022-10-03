from airflow.decorators import dag, task
from datetime import datetime, timedelta

from utils.df_s3file import DataFrameS3File

default_args = {
    'owner': 'Freakisimo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='process_pedestrian_accidents_data',
    default_args=default_args,
    start_date=datetime(2022,10,2),
    schedule_interval='@daily'
)
def process_gtfs_data():

    dfs3f = DataFrameS3File()
    bucket = 'datos-cdmx'

    @task
    def process_data():
        accidents = dfs3f.get_gpd(bucket, 'accidentado_peaton.shp')

        print(accidents.columns)
        return None


    process_data()

process_gtfs_dag = process_gtfs_data()
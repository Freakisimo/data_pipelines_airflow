from airflow.decorators import dag, task
from datetime import datetime, timedelta

from utils.df_s3file import DataFrameS3File

default_args = {
    'owner': 'Freakisimo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='process_gtfs_data',
    default_args=default_args,
    start_date=datetime(2022,8,29),
    schedule_interval='@daily'
)
def process_gtfs_data():

    dfs3f = DataFrameS3File()
    bucket = 'datos-cdmx'
    csv_files = ['agency', 'routes', 'shapes', 'stops']

    @task
    def process_data():
        dfs = {name: dfs3f.get_df(bucket, name) for name in csv_files}
        for df in dfs.values():
            print(df.head())
        return None


    process_data()

process_gtfs_dag = process_gtfs_data()
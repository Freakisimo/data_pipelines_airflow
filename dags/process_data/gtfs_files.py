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
    start_date=datetime(2022,9,7),
    schedule_interval='@daily'
)
def process_gtfs_data():

    dfs3f = DataFrameS3File()
    bucket = 'datos-cdmx'
    csv_files = ['agency', 'routes', 'shapes', 'stops']

    @task
    def process_data():
        # dfs = {name: dfs3f.get_df(bucket, name) for name in csv_files}
        # for df in dfs.values():
        #     print(df.head())
        agency_df = dfs3f.get_df(bucket, 'agency')
        shapes_df = dfs3f.get_df(bucket, 'shapes')
        routes_df = dfs3f.get_df(bucket, 'routes')
        stops_df = dfs3f.get_df(bucket, 'stops')
        stop_times_df = dfs3f.get_df(bucket, 'stop_times')
        trips_df = dfs3f.get_df(bucket, 'trips')

        print(agency_df.columns)
        print(shapes_df.columns)
        print(routes_df.columns)
        print(stops_df.columns)
        print(trips_df.columns)
        print(stop_times_df.columns)
        return None


    process_data()

process_gtfs_dag = process_gtfs_data()
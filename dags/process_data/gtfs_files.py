from itertools import groupby
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
    start_date=datetime(2022,9,1),
    schedule_interval='@monthly'
)
def process_gtfs_data():

    dfs3f = DataFrameS3File()
    bucket = 'datos-cdmx'

    @task
    def process_data():
        import pandas as pd
        import json

        agency_df = dfs3f.get_df(bucket, 'agency')
        shapes_df = dfs3f.get_df(bucket, 'shapes')
        routes_df = dfs3f.get_df(bucket, 'routes')
        stops_df = dfs3f.get_df(bucket, 'stops')
        stop_times_df = dfs3f.get_df(bucket, 'stop_times')
        trips_df = dfs3f.get_df(bucket, 'trips')
        
        agency_routes = pd.merge(agency_df, routes_df, on='agency_id', how='left')
        routes_trips = pd.merge(agency_routes, trips_df, on='route_id', how='left')

        stops = pd.merge(stops_df, stop_times_df, on='stop_id')
        routes_stops = pd.merge(routes_trips, stops, on='trip_id', how='inner')

        valid_columns = [
            'agency_name',
            'route_id','route_short_name','route_long_name','route_color',
            'trip_id','trip_short_name','direction_id',
            'stop_id','stop_name','stop_lat','stop_lon'
        ]

        routes_stops = routes_stops[valid_columns]

        gtfs_dict = routes_stops.groupby([
                'agency_name',
                'route_id','route_short_name','route_long_name', 'route_color',
                'trip_id','trip_short_name','direction_id',
            ])[['stop_id','stop_name','stop_lat','stop_lon']] \
            .apply(lambda x: x.set_index('stop_id').to_dict(orient='records')) \
            .reset_index() \
            .rename(columns={0:'stops'})\
            .groupby([
                'agency_name',
                'route_id','route_short_name','route_long_name', 'route_color',
            ])[['trip_id','trip_short_name','direction_id','stops']] \
            .apply(lambda x: x.set_index('trip_id').to_dict(orient='records')) \
            .reset_index() \
            .rename(columns={0:'trips'})\
            .groupby(
                'agency_name',
            )[['route_id','route_short_name','route_long_name', 'route_color','trips']] \
            .apply(lambda x: x.set_index('route_id').to_dict(orient='records')) \
            .reset_index() \
            .rename(columns={0:'routes'})\
            .to_dict(orient='records')

        print(json.dumps(gtfs_dict))

        return None


    process_data()

process_gtfs_dag = process_gtfs_data()
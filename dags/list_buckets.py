from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Freakisimo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='list_buckets_s3',
    default_args=default_args,
    start_date=datetime(2022,8,9),
    schedule_interval='@daily'
)
def list_buckets():

    @task()
    def all_buckets():
        import boto3
        
        s3 = boto3.resource('s3')
        print(s3)
        for bucket in s3.buckets.all():
            print(bucket.name)

    all_buckets()

s3_dag = list_buckets()
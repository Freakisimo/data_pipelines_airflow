from airflow.decorators import dag, task
from datetime import datetime, timedelta

from utils.python_tools import download_files, unzip_files, \
    upload_s3_files, xlsx_to_csv

default_args = {
    'owner': 'Freakisimo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='get_files_accidentes_peatones',
    default_args=default_args,
    start_date=datetime(2022,8,15),
    schedule_interval='@daily'
)
def etl_data_cdmx():

    @task()
    def extract():
        return download_files(
            url='https://datos.cdmx.gob.mx/dataset/puntos-de-accidentes-a-peatones',
            path='/tmp/accidentes_peatones/',
            label='a',
            css_class='resource-url-analytics'
        )
    
    @task()
    def transform(files):
        return unzip_files(
            files=files
        )
        

    @task()
    def load(local_files):
        upload_s3_files(
            files=local_files,
            start_path='/tmp/',
            bucket='datos-cdmx'
        )


    files = extract()
    local_files = transform(files)
    load(local_files)

gtfs_dag = etl_data_cdmx()
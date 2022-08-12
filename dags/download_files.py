from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Freakisimo',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def download_files(url, path, label, css_class=None):
    from bs4 import BeautifulSoup
    import requests
    import os

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    css_condition = {"class": css_class} if css_class else {}
    links = [a['href'] for a in soup.find_all(label, css_condition)]

    if not os.path.exists(path):
        os.mkdir(path)

    for link in links:
        file = requests.get(link, stream = True)
        file_name = link.rsplit('/', 1)[-1]

        with open(path + file_name,"wb") as shp:
            for chunk in file.iter_content(chunk_size=1024):  
                if chunk:
                    shp.write(chunk)


with DAG(
    default_args=default_args,
    dag_id='download_data_cdmx_files',
    description='Download GTFS zip files from datos cdmx',
    start_date=datetime(2022, 8, 8),
    schedule_interval='@daily'
) as dag:
    tasks1 = PythonOperator(
        task_id='download_gtfs',
        python_callable=download_files,
        op_kwargs={
            'url': 'https://datos.cdmx.gob.mx/dataset/gtfs',
            'path': '/tmp/gtfs/',
            'label': 'a',
            'css_class': 'resource-url-analytics'
        }
    )

    tasks2 = PythonOperator(
        task_id='download_cetram',
        python_callable=download_files,
        op_kwargs={
            'url': 'http://datos.cdmx.gob.mx/dataset/ubicacion-de-centros-de-transferencia-modal-cetram',
            'path': '/tmp/cetram/',
            'label': 'a',
            'css_class': 'resource-url-analytics'
        }
    )

    tasks3 = PythonOperator(
        task_id='download_c5',
        python_callable=download_files,
        op_kwargs={
            'url': 'https://datos.cdmx.gob.mx/dataset/incidentes-viales-c5',
            'path': '/tmp/c5/',
            'label': 'a',
            'css_class': 'resource-url-analytics'
        }
    )

    tasks4 = PythonOperator(
        task_id='download_ingresos_metro',
        python_callable=download_files,
        op_kwargs={
            'url': 'https://datos.cdmx.gob.mx/dataset/ingresos-del-sistema-de-transporte-colectivo-metro',
            'path': '/tmp/ingresos_metro/',
            'label': 'a',
            'css_class': 'resource-url-analytics'
        }
    )

    tasks1 >> tasks2 >> tasks3 >> tasks4
from bs4 import BeautifulSoup
from typing import List
import requests
import os
import zipfile
import boto3

def download_files(url: str, path: str, label: str, css_class=None) -> List[str]:

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    css_condition = {"class": css_class} if css_class else {}
    links = [a['href'] for a in soup.find_all(label, css_condition)]

    if not os.path.exists(path):
        os.mkdir(path)

    files = []
    for link in links:
        file = requests.get(link, stream = True)
        file_name = link.rsplit('/', 1)[-1]
        file_path = f"{path}{file_name}"
        with open(file_path,"wb") as shp:
            for chunk in file.iter_content(chunk_size=1024):  
                if chunk:
                    shp.write(chunk)
        
        files.append(file_path)

    return files    


def unzip_files(files: list) -> List[str]:
    local_files = []
    for f in files:
        if f.endswith('.zip'):
            f_path = os.path.dirname(f)
            target_path = f"{f_path}/"
            with zipfile.ZipFile(f,"r") as zip_ref:
                zip_files = [f"{target_path}{f}" for f in zip_ref.namelist()]
                zip_ref.extractall(target_path)

            files.remove(f)

            if os.path.exists(f):
                os.remove(f)
            
            local_files += zip_files
            
    return local_files


def upload_s3_files(files: list, bucket: str) -> None:
    s3_client = boto3.client('s3')
    for f in files:
        object_name = os.path.basename(f)
        try:
            s3_client.upload_file(f, bucket, object_name)
        except Exception as e:
            print(e)

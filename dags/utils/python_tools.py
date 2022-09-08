from bs4 import BeautifulSoup
import requests
import os
import re
import shutil
import zipfile
import boto3
import pandas as pd


def safe_path(path: str) -> str:
    to_lower = path.lower()
    spaces = to_lower.replace(" ", "_")
    return spaces


def download_files(
    url: str, 
    path: str, 
    label: str, 
    css_class: str=None, 
    innertext: str=None
) -> list:


    if os.path.exists(path):
        shutil.rmtree(path)

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    css_condition = {"class": re.compile(f'.*{css_class}*.')} if css_class else {}
    text_condition = re.compile(f'.*{innertext}*.')
    links = [a['href'] for a in soup.find_all(label, css_condition, text=text_condition)]

    if not os.path.exists(path):
        os.mkdir(path)

    files = []
    for link in links:
        file = requests.get(link, stream = True)
        file_name = link.rsplit('/', 1)[-1]
        file_path = safe_path(f"{path}{file_name}")
        with open(file_path,"wb") as shp:
            for chunk in file.iter_content(chunk_size=1024):  
                if chunk:
                    shp.write(chunk)
        
        files.append(file_path)

    return files    


def unzip_files(files: list) -> list:
    local_files = []
    for f in files:
        if f.endswith('.zip'):
            f_path = os.path.dirname(f)
            target_path = f"{f_path}/"
            with zipfile.ZipFile(f,"r") as zip_ref:
                zip_files = [f"{target_path}{zf}" for zf in zip_ref.namelist()]
                zip_ref.extractall(target_path)

            files.remove(f)

            if os.path.exists(f):
                os.remove(f)
            
            local_files += zip_files
        
    local_files += files

    local_files = [lf for lf in local_files if os.path.isfile(lf)]
    local_files = [lf for lf in local_files if '__MACOSX' not in lf]

    for idx,lf in enumerate(local_files, start=0):
        lf_safe = safe_path(lf)
        dir_path = os.path.dirname(lf_safe)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        os.rename(lf, lf_safe)
        local_files[idx] = lf_safe

    return local_files


def xlsx_to_csv(files: list) -> list:
    local_files = []
    for f in files:
        if f.endswith('.xlsx') or f.endswith('.xls'):
            path = os.path.splitext(f)[0]
            read_file = pd.read_excel(f)
            read_file.to_csv (f'{path}.csv', index=None, header=True)

            files.remove(f)

            if os.path.exists(f):
                os.remove(f)

            local_files.append(f'{path}.csv')

    local_files += files

    return local_files 
    

def upload_s3_files(files: list, start_path: str, bucket: str, prefix: str='') -> None:
    s3_client = boto3.client('s3')
    for f in files:
        r_path = os.path.relpath(f, start_path)
        object_name = f"{prefix}{r_path}"
        try:
            s3_client.upload_file(f, bucket, object_name)
        except Exception as e:
            print(e)


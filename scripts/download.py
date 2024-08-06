import os
from urllib.request import urlretrieve
from typing import List

def make_directories(output_relative_dir: str, target_dirs: List[str]):

    if not os.path.exists(output_relative_dir):
        os.makedirs(output_relative_dir)
    

    for td in target_dirs:
        if not os.path.exists(output_relative_dir + td):
            os.makedirs(output_relative_dir + td)

def download_files(url_template: str, output_dir: str, years: List[int], months: List[int]):
    for year in years:
        for month in months:
            month = str(month).zfill(2) 
            print(f"Begin month {month}")
        
            # generate url
            url = f'{url_template}{year}-{month}.parquet'
            # generate output location and filename
            output_dir = f"{output_dir}/{year}-{month}.parquet"
            # download
            urlretrieve(url, output_dir) 
            
            print(f"Completed month {month}")

import os
from urllib.request import urlretrieve

def make_directories(output_relative_dirs: list[str], target_dirs: list[str]):
    """Create directories for data"""
    for ord in output_relative_dirs:
        if not os.path.exists(ord): # check if directories already exist
            os.makedirs(ord)
        for td in target_dirs:
            if not os.path.exists(ord + td): # check if directories already exist
                os.makedirs(ord + td)

def download_files(url_template: str, output_dir: str, year: int, months: list[int]):
    """Download data from url"""
    for month in months:
        month = str(month).zfill(2)
        print(f"Begin month {month}")
        # generate url
        url = f'{url_template}{year}-{month}.parquet'
        print(url)
        # generate output location and filename
        output_file = f"{output_dir}/{year}-{month}.parquet"
        # download
        retrieve_file(url, output_file)            
        print(f"Completed month {month}")

def retrieve_file(url: str, output_file: str):
    """Retrieve data to output file"""
    urlretrieve(url, output_file)


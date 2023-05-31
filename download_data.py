import os
import itertools
import requests
import gzip
import shutil

def generate_urls(year, month):
    pattern = "https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz"
    month = str(month).zfill(2)
    days = [str(day).zfill(2) for day in range(1, 32)]
    hours = [str(hour) for hour in range(24)]
    # Generate urls
    combinations = itertools.product(days, hours)
    urls = [pattern.format(year=year, month=month, day=day, hour=hour) for day, hour in combinations]
    return urls

def download_extract_file(file_url, output_file):
    r = requests.get(file_url, stream=True)

    if r.status_code == 200:
        with open(output_file, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    with gzip.open(output_file, 'rb') as f_in, open(output_file[:-3], 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def download_month(year, month):
    os.makedirs('downloads', exist_ok=True)
    urls = generate_urls(year, month)

    for url in urls:
        print('Downloading {}'.format(url))
        output_file = 'downloads/{}'.format(url[27:])
        download_extract_file(url, output_file)

download_month(2015, 1)

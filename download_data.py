import requests
import gzip
import shutil

def download_extract(file_url, output_file):
    r = requests.get(file_url, stream=True)

    if r.status_code == 200:
        with open(output_file, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    with gzip.open(output_file, 'rb') as f_in, open('new_file.json', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

file_url = "https://data.gharchive.org/2015-01-01-15.json.gz"
output_file = 'git_data.gz'

download_extract(file_url, output_file)
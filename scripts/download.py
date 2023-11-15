import os
import shutil

import wget
import gzip

url = 'https://data.ris.ripe.net'


def construct_output_path(folder, rrc, year, month):
    return f"{folder}/{rrc}/{year}.{month:02d}/"


def construct_updates_filename(folder, rrc, year, month, date, step=None):
    if step:
        return f"{folder}/{rrc}/{year}.{month:02d}/updates.{date}.{step}"
    return f"{folder}/{rrc}/{year}.{month:02d}/{date}"


def construct_updates_filename_url(rrc, year, month, date, step=None):
    return f"{url}/{rrc}/{year}.{month:02d}/updates.{date}.{step}.gz"


def download_updates_file(file_url, output_path):
    try:
        filename = wget.download(file_url, output_path)
        decompresses_file = filename.split('.gz')[0]
        with gzip.open(filename, 'rb') as f_in:
            with open(decompresses_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(filename)
    except Exception as e:
        print(f"Download failed for file_url: {file_url}", e)

import os
import requests
from retrying import retry
from datetime import datetime

# Function to download a file with retry logic
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def download_file(url, save_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
    else:
        response.raise_for_status()

# Main function to download files for Year 2019
def download_trip_data(year=2019):
    base_url = f'https://example.com/trip_data/{year}/'  # Replace with actual base URL
    save_directory = f'./trip_data_{year}/'  # Replace with desired save directory

    os.makedirs(save_directory, exist_ok=True)

    # Assuming files are named in a structured way, like trip_data_2019_01.csv, trip_data_2019_02.csv, etc.
    for month in range(1, 13):
        filename = f'trip_data_{year}_{month:02d}.csv'
        url = base_url + filename
        save_path = os.path.join(save_directory, filename)

        try:
            print(f'Downloading {filename}...')
            download_file(url, save_path)
            print(f'{filename} downloaded successfully.')
        except Exception as e:
            print(f'Error downloading {filename}: {str(e)}')

if __name__ == "__main__":
    download_trip_data()

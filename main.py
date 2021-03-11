import os
import time
import random
import dotenv
import requests
import psycopg2
import concurrent.futures


current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_path)

dotenv.load_dotenv()

connection = psycopg2.connect(
    database=os.getenv('POSTGRES_DB'), user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'), host="127.0.0.1", port="5432",
)


def setup_db() -> None:
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (id SERIAL, text text);
    """)


def process_data(data: str) -> str:
    start = time.perf_counter()
    sleep_time = random.randrange(1, 10)
    time.sleep(sleep_time)
    finish = time.perf_counter()
    execution_time = round(finish - start, 2)
    print(f'Data was processed in {execution_time} second')
    return data


def download_data_by_http(http_address: str) -> None:
    response = requests.request("GET", http_address)
    processed_data = process_data(response.text)
    cursor = connection.cursor()
    cursor.execute('INSERT INTO results (text) VALUES (%s)', [processed_data])


def chunks(lst: list, n: int) -> list:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def read_file(path: str, parallel_downloads: int) -> None:
    with open(path, mode='r') as file:
        line_chunks = list(chunks(file.readlines(), parallel_downloads))

        for url_chunk in line_chunks:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(download_data_by_http, url_chunk)


if __name__ == '__main__':
    setup_db()
    filepath = input(
        'Please enter path to a file. If an entry does not exist or is not a file, default file will be used: '
    )
    is_valid_integer = False
    parallel_processes = 0
    input_text = 'How many parallel processes would you like to initiate? Available range: from 4 to 32. '
    while not is_valid_integer:
        parallel_processes = input(
            input_text
        )
        try:
            parallel_processes = int(parallel_processes)
            if not 4 <= parallel_processes <= 32:
                input_text = 'Number of parallel processes should be in the range between 4 and 32. '
            else:
                is_valid_integer = True
        except ValueError:
            input_text = 'The input doesn\'t appear to be a number. Please enter number between 4 and 32. '

    if not filepath or not os.path.exists(filepath) or not os.path.isfile(filepath):
        filepath = os.path.join(current_dir, 'sample.txt')
    read_file(filepath, parallel_processes)

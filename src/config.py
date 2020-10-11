import os
from os.path import dirname, abspath

ROOT_DIR = dirname(dirname(abspath(__file__)))
DATABASE_TYPE = 'bigquery'
CITY = 'hanoi'
API_URL = 'https://api.waqi.info/feed/'
TOKEN = 'edb89de2ed7b855dc7ba46713cc653716299226b'
INTERVAL = 60
DATABASE_NAME = 'AQI'
TABLE_NAME = 'hanoi'
PROJECT_NAME = 'sonorous-folio-272202'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "bigqueryapi.json"

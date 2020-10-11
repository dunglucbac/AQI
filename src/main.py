import logging
import datetime

from google.cloud import bigquery

from data_api import DataAPI
from process_db import WeatherBigquery
from config import (DATABASE_NAME, TABLE_NAME, API_URL, 
                        TOKEN, CITY, PROJECT_NAME)


logger = logging.getLogger('AQI')
schema = [
    bigquery.SchemaField("ID", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Time", "Time"),
    bigquery.SchemaField("Date", "DATE"),
    bigquery.SchemaField("City", "STRING"),
    bigquery.SchemaField("AQI", "FLOAT64"),
    bigquery.SchemaField("DominentPol", "STRING"),
    bigquery.SchemaField("CO", "FLOAT64"),
    bigquery.SchemaField("NO2", "FLOAT64"),
    bigquery.SchemaField("O3", "FLOAT64"),
    bigquery.SchemaField("PM25", "FLOAT64"),
    bigquery.SchemaField("Dew", "FLOAT64")
]


def get_and_update_db(data_api, weather_db, previous_clean_data):
    """
    This function is passed 3 arguments from  DataAPI and WeatherBigquery class
    then calling each class method on them in order to process data.
    Parmameters: 
        data_api, weather_db: 2 instances created by 2 classes
        (list) previous_clean_data: query data from Bigquery to prevent duplicated data
    Return:
        I will not return anything unless it finds the data from past equals the current data.
    """
    raw_data = data_api.get_data()
    (sql_data, clean_data) = data_api.process_data(raw_data)
    logger.debug("Try to compare for inserting decision:")
    if clean_data == previous_clean_data:
        logger.debug("Same data! Wait for More!")
        return
    try:
        current_index = weather_db.current_index()
        data_list = ",".join(map(str, sql_data))
        insert_sql = "INSERT INTO {}.{} VALUES ({},{})".format(
            DATABASE_NAME, TABLE_NAME, current_index, data_list)
        weather_db.insert(insert_sql, current_index)
        logger.debug("Data added at {} at row id = {}".format(
            datetime.datetime.now(), current_index - 1))
    except:
        logger.debug("Error collecting data at " +
                     str(datetime.datetime.now()))


if __name__ == '__main__':
    weather_db = WeatherBigquery(DATABASE_NAME, TABLE_NAME, PROJECT_NAME)
    logger.debug("Connected Database!")
    weather_db.init_table(schema)

    data_api = DataAPI(TOKEN, API_URL, CITY)

    previous_clean_data = weather_db.select_last_row()
    get_and_update_db(data_api, weather_db, previous_clean_data)
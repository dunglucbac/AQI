import json
import requests


class DataAPI:
    def __init__(self, token, api_url, city):
        self.token = token
        self.api_url = api_url
        self.city = city

    def get_data(self):
        """
        The method return json data from the request using token, city and api_url
        """
        url = self.api_url + self.city + "/?token=" + self.token
        try:
            response = requests.get(url)
        except:
            return []
        raw = response.json()
        return raw['data']

    @staticmethod
    def data_filter(raw_data, p):
        """
        The helper method will return the value of p attribute of the raw data in the nested dictionary json
        """
        iaqi = raw_data.get('iaqi')
        if iaqi:
            pollutant = iaqi.get(p)
            if pollutant:
                return pollutant.get('v')
        return -1

    def process_data(self, raw_data):
        """
        The helper method will use data_filter helper method to get all interesting values based on our database structure
        input: raw_data - json dictionary object return from get_data method
        """
        time = raw_data['time']['s'].split(" ")[1]
        date = raw_data['time']['s'].split(" ")[0]
        city = self.city.capitalize()
        aqi = float(raw_data['aqi'])

        dominentPol = raw_data['dominentpol'].upper()
        co = float(DataAPI.data_filter(raw_data, 'co'))
        no2 = float(DataAPI.data_filter(raw_data, 'no2'))
        o3 = float(DataAPI.data_filter(raw_data, 'o3'))
        pm25 = float(DataAPI.data_filter(raw_data, 'pm25'))
        dew = float(DataAPI.data_filter(raw_data, 'dew'))

        sql_data = [
            repr(time),
            repr(date),
            repr(city), aqi,
            repr(dominentPol), co, no2, o3, pm25, dew
        ]
        clean_data = [
            time, date, city, aqi, dominentPol, co, no2, o3, pm25, dew
        ]
        return (sql_data, clean_data)

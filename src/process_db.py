import logging

from google.cloud import bigquery


logger = logging.getLogger('AQI')


class WeatherBigquery:
    def __init__(self, database_name, table_name, project_name):
        self.project_name = project_name
        self.client = bigquery.Client()
        self.database_name = database_name
        self.table_name = table_name

    def init_table(self, schema):
        try:
            table_id = "{}.{}.{}".format(self.project_name, self.database_name, self.table_name)
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)  # Make an API request.
            logger.debug(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        except Exception as err:
            logger.debug(err)

    def current_index(self):
        """
        This method is to return the current ID index of the lastest row
        """
        job = self.client.query("SELECT MAX(ID) FROM {}.{};".format(self.database_name, self.table_name))
        for row in job.result():
            if row[0] == None:
                return 1
            current_index = row[0] + 1
        return current_index

    def select_last_row(self):
        """
        This method is to select the last row
        """
        previous_list_data = []
        job = self.client.query(
            "SELECT * FROM {}.{} ORDER BY ID DESC LIMIT 1;".format(self.database_name, self.table_name))
        for row in job:
            previous_list_data = [row.Time.strftime("%H:%M:%S"), row.Date.strftime("%Y-%m-%d"), row.City, row.AQI,
                                            row.DominentPol, row.CO, row.NO2, row.O3, row.PM25, row.Dew]
        if previous_list_data:
            return previous_list_data

    def insert(self, sql_query, current_index):
        """
        This method is to insert based on sql_query and current_index and return current_index + 1 if success
        """
        self.client.query(sql_query)
        return current_index + 1
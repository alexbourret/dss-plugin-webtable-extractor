from dataiku.connector import Connector
from webtable_extractor_commons import RecordsLimit, safe_get
import pandas
import json


class WebTableConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.tables_url = self.config.get("tables_url")
        self.table_to_extract = self.config.get("table_to_extract")
        if not self.tables_url:
            raise Exception("Please enter a valid URL")
        if self.table_to_extract is None:
            raise Exception("Please select the table to extract")

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """

        # In this example, we don't specify a schema here, so DSS will infer the schema
        # from the columns actually returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        limit = RecordsLimit(records_limit)
        data, error_message = safe_get(self.tables_url)
        if error_message:
            raise Exception(error_message)
        dataframes = pandas.read_html(data, header=0) #, extract_links="all")
        dataframe = dataframes[self.table_to_extract].to_json(orient="records")
        json_data = json.loads(dataframe)
        for row in json_data:
            yield row
            if limit.is_reached():
                return


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        raise NotImplementedError


    def get_partitioning(self):
        raise NotImplementedError


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError


    def get_records_count(self, partitioning=None, partition_id=None):
        raise NotImplementedError

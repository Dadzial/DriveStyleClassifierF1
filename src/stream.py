from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSourceStreamReader, DataSource, InputPartition
from pyspark.sql.types import StructType
from typing import Iterator, Tuple
import requests

class F1StreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        self._schema = schema
        self._options = options
        self.current_offset = 0
        # URL do OpenF1
        self.base_url = "https://api.openf1.org/v1/car_data?driver_number=55&session_key=9159&speed>=315"

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        self.current_offset += 1
        return {"offset": self.current_offset}

    def partitions(self, start: dict, end: dict):
        return [InputPartition(0)]

    def commit(self, end: dict):
        pass

    def read(self, partition) -> Iterator[Tuple]:
        """Pobieranie danych i mapowanie na schemat Sparka"""
        try:
            response = requests.get(self.base_url, timeout=10)
            data = response.json()

            if isinstance(data, list):
                for record in data:
                    yield (
                        record.get('brake'),
                        record.get('date'),
                        record.get('driver_number'),
                        record.get('drs'),
                        record.get('meeting_key'),
                        record.get('n_gear'),
                        record.get('rpm'),
                        record.get('session_key'),
                        record.get('speed'),
                        record.get('throttle')
                    )
        except Exception as e:
            print(f"Błąd pobierania danych: {e}")

class F1DataSource(DataSource):
    @classmethod
    def name(cls):
        return "f1"

    def schema(self):
        return """
            brake int,
            date string,
            driver_number int,
            drs int,
            meeting_key int,
            n_gear int,
            rpm int,
            session_key int,
            speed double,
            throttle double
        """

    def streamReader(self, schema: StructType):
        return F1StreamReader(schema, self.options)

spark.dataSource.register(F1DataSource)


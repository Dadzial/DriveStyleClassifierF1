from pyspark.sql.datasource import DataSourceStreamReader, DataSource, InputPartition
from pyspark.sql.types import StructType
from typing import Iterator, Tuple
import requests


class F1StreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        self._schema = schema
        self._options = options
        self.records_per_batch = 10
        self.current_off = 0

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:

        self.current_off += self.records_per_batch
        return {"offset": self.current_off}

    def partitions(self, start: dict, end: dict):
        s = start["offset"]
        e = end["offset"]
        return [InputPartition(i) for i in range(s, e)]

    def read(self, partition) -> Iterator[Tuple]:
        url = "https://api.openf1.org/v1/car_data?driver_number=55&session_key=9159&speed>=1"

        try:
            response = requests.get(url, timeout=10)
            data = response.json()

            idx = partition.value

            if isinstance(data, list) and idx < len(data):
                record = data[idx]

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
            print(f"Błąd w read: {e}")


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
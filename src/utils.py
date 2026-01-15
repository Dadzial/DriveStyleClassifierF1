from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Utworzenie funkcji zwracającej lokalizację punktów kontrolnych zapisu na podstawie lokalizacji bieżącej bazy danych używanej w ramach sesji.

def get_checkpoint_location(session: SparkSession) -> StringType:
    current_database = session.sql("SELECT current_database()").first()[0]
    database_location = (
        session
            .sql(f"DESCRIBE DATABASE EXTENDED {current_database}")
            .filter("database_description_item = 'Location'")
            .select("database_description_value")
            .first()["database_description_value"]
        )
    return database_location + "/tmp/checkpoint/"

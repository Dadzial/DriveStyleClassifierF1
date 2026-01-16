import os
import findspark
from pyspark.sql.connect.streaming.query import StreamingQuery
from stream import F1DataSource
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-21-openjdk-amd64'
findspark.init("/opt/spark")


session = SparkSession.builder \
    .master("local[*]") \
    .appName("DriveStyleClassifier") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()

def init_db(session: SparkSession) -> str:
    session.sql("DROP DATABASE IF EXISTS drive_style CASCADE")
    session.sql("CREATE DATABASE IF NOT EXISTS drive_style")
    session.sql("USE drive_style")
    return session.catalog.currentDatabase()


def start_streaming_drive_data(session: SparkSession, watermarks: bool = True) -> StreamingQuery:
    df_stream = session.readStream.format("f1").load()

    if watermarks:
        df_stream = (
            df_stream
            .withColumn("date", F.to_timestamp("date"))
            .withWatermark("date", "180 seconds")
        )

    query = (
        df_stream.writeStream
        .format("console")
        .trigger(processingTime="7 seconds")
        .outputMode("append")
        .start()
    )

    return query

def agg_speed_rpm(df, window_sec=5):
    df = df.withColumn("ts", F.to_timestamp("date"))
    return df.groupBy(
        "driver_number",
        F.window("ts", f"{window_sec} seconds")
    ).agg(
        F.avg("speed").alias("avg_speed"),
        F.avg("rpm").alias("avg_rpm"),
        (F.max("speed") - F.min("speed")).alias("delta_speed"),
        (F.max("rpm") - F.min("rpm")).alias("delta_rpm")
    )


if __name__ == "__main__":

    session.dataSource.register(F1DataSource)
    current_db = init_db(session)
    print(f"Current database: {current_db}")

    print("Startowanie streamu...")
    active_query = start_streaming_drive_data(session)

    active_query.awaitTermination()
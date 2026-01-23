import os
import findspark
from stream import F1DataSource
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import time

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


def start_streaming_drive_data(session: SparkSession, watermarks: bool = True):
    df_stream = session.readStream.format("f1").load()
    df_stream = df_stream.withColumn("date", F.to_timestamp("date"))

    if watermarks:
        df_stream = df_stream.withWatermark("date", "180 seconds")


    raw_query = (
        df_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Sliding window features
    features = build_features(df_stream)
    agg_query = (
        features.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    return raw_query, agg_query


def build_features(df):
    return (
        df
        .withColumn("ts", F.col("date"))
        .groupBy(
            "driver_number",
            "session_key",
            F.window("ts", "15 seconds", "5 seconds")
        )
        .agg(
            F.round(F.avg("speed"), 2).alias("avg_speed"),
            F.round((F.max("speed") - F.min("speed")), 2).alias("speed_trend"),
            F.round(F.stddev("rpm"), 2).alias("rpm_stability"),
            F.round(F.avg("throttle"), 2).alias("avg_throttle"),
            F.round(F.avg("brake"), 2).alias("avg_brake"),
            F.round(F.avg("drs"), 2).alias("avg_drs")
        )
    )


if __name__ == "__main__":
    session.dataSource.register(F1DataSource)
    current_db = init_db(session)
    print(f"Current database: {current_db}")

    print("Startowanie streamu...")
    raw_query, agg_query = start_streaming_drive_data(session)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        raw_query.stop()
        agg_query.stop()

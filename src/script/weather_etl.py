from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os
import sys


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_weather_data(spark, input_data, output_data):
    weather_data = os.path.join(input_data, 'weather_data/weather_paris.json')

    df_weather = spark.read.json(weather_data)

    # Concert timestamp to date
    df_weather = df_weather.withColumn("time", F.to_timestamp(F.col("time") / 1000))
    df_weather = df_weather.withColumn("time", F.to_date(F.col("time")))

    # Extract columns to create weather table
    weather_table = df_weather.select(['time', 'tavg', 'tmin', 'tmax']) \
        .withColumn('month', F.month('time')) \
        .withColumn('year', F.year('time')) \
        .withColumn('day', F.dayofmonth('time')) \
        .withColumnRenamed('time', 'date')

    # Write weather table
    weather_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'weather_table'), 'overwrite')


def main():
    input_data = sys.argv[1]
    output_data = sys.argv[2]

    spark = create_spark_session()

    process_weather_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

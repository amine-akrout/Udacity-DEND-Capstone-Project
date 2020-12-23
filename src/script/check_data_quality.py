import os
from pyspark.sql import SparkSession
import sys


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def check_covid_table(spark, datalake_bucket):
    df_covid = spark.read.parquet(os.path.join(datalake_bucket, 'covid_table/year=*/month=*/*.parquet'))

    if df_covid.count() == 0:
        raise AssertionError('Covid table is empty !')


def check_host_table(spark, datalake_bucket):
    df_host = spark.read.parquet(os.path.join(datalake_bucket, 'host_table/*.parquet'))

    if df_host.count() == 0:
        raise AssertionError('Host table is empty !')


def check_listing_table(spark, datalake_bucket):
    df_listing = spark.read.parquet(os.path.join(datalake_bucket, 'listing_table/*.parquet'))

    if df_listing.count() == 0:
        raise AssertionError('Listing table is empty !')


def check_weather_table(spark, datalake_bucket):
    df_weather = spark.read.parquet(os.path.join(datalake_bucket, 'weather_table/year=*/month=*/*.parquet'))

    if df_weather.count() == 0:
        raise AssertionError('Weather table is empty !')


def check_time_table(spark, datalake_bucket):
    df_time = spark.read.parquet(os.path.join(datalake_bucket, 'date_table/*.parquet'))

    if df_time.count() == 0:
        raise AssertionError('Time table is empty !')


def check_fact_revenue_table(spark, datalake_bucket):
    df_fact_revenue = spark.read.parquet(os.path.join(datalake_bucket, 'fact_revenue_table/year=*/month=*/*.parquet'))

    if df_fact_revenue.count() == 0:
        raise AssertionError('Fact Revenue table is empty !')


def main():
    datalake_bucket = sys.argv[1]

    spark = create_spark_session()

    check_covid_table(spark, datalake_bucket)
    check_host_table(spark, datalake_bucket)
    check_listing_table(spark, datalake_bucket)
    check_weather_table(spark, datalake_bucket)
    check_time_table(spark, datalake_bucket)
    check_fact_revenue_table(spark, datalake_bucket)


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import os
import sys


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_airbnb_data(spark, input_data, output_data):
    airbnb_data = os.path.join(input_data, 'airbnb_paris_data/')

    # import reviews data

    df_review = spark.read.format('csv') \
        .option("inferSchema", "True") \
        .option("header", "True") \
        .option("sep", ",") \
        .option('escape', '"') \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .option("encoding", "ISO-8859-1") \
        .load(airbnb_data + 'reviews.csv')

    avg_length_of_stay_paris = 5.2
    review_rate_modest = 0.5

    # calculate occupancy rate
    df_review = df_review.groupBy("listing_id", "date") \
        .count() \
        .withColumnRenamed("count", "review_day") \
        .withColumn('occupancy', F.round(avg_length_of_stay_paris * (F.col('review_day') / review_rate_modest), 2)) \
        .filter(F.col('date') >= "2016-11-01")

    # import listing data
    df_listing = spark.read.format('csv') \
        .option("inferSchema", "True") \
        .option("header", "True") \
        .option("sep", ",") \
        .option('escape', '"') \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .option("encoding", "ISO-8859-1") \
        .load(airbnb_data + 'listings.csv')

    host_table_temp = df_listing.select(
        ['id', 'host_id', 'host_name', 'host_location', 'host_since', 'host_response_time', 'host_response_rate',
         'host_is_superhost', 'host_acceptance_rate']).dropDuplicates()

    # create host table
    host_table = host_table_temp.drop('id').dropDuplicates()

    # create listing table
    listing_table = df_listing.select(
        ['id', 'name', 'description', 'price', 'neighbourhood_group_cleansed', 'latitude', 'longitude', 'property_type',
         'accommodates', 'bathrooms', 'bedrooms', 'beds']) \
        .dropDuplicates() \
        .withColumnRenamed("id", "id_listing")

    listing_temp = listing_table.select(['id_listing', 'price']) \
        .withColumn("price", F.expr("substring(price,2,length(price)-1)")) \
        .withColumn("price", F.col("price").cast(DoubleType()))

    # create fact revenue table
    fact_revenue_table = df_review.join(listing_temp, (df_review.listing_id == listing_temp.id_listing)) \
        .join(host_table_temp, (df_review.listing_id == host_table_temp.id)) \
        .drop('id_listing') \
        .drop('id') \
        .withColumn('income_est', F.round(F.col('occupancy') * F.col('price'), 2)) \
        .select(['date', 'listing_id', 'host_id', 'occupancy', 'income_est']) \
        .withColumn('year', F.year('date')) \
        .withColumn('month', F.month('date')) \
        .withColumn('day', F.dayofmonth('date'))

    # create time table
    time_table = fact_revenue_table.select(['date', 'year', 'month', 'day']) \
        .withColumn('weekday', F.dayofweek('date')) \
        .dropDuplicates()

    # Write host table
    host_table.write.parquet(os.path.join(output_data, 'host_table'), 'overwrite')
    # Write listings table
    listing_table.write.parquet(os.path.join(output_data, 'listing_table'), 'overwrite')
    # Write time table
    time_table.write.parquet(os.path.join(output_data, 'date_table'), 'overwrite')
    # Write fact table
    fact_revenue_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'fact_revenue_table'),'overwrite')


def main():
    input_data = sys.argv[1]
    output_data = sys.argv[2]

    spark = create_spark_session()

    process_airbnb_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

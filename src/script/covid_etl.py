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


def to_long(df, by):
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = F.explode(F.array([
        F.struct(F.lit(c).alias("key"), F.col(c).alias("val")) for c in cols
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])


def process_covid_data(spark, input_data, output_data):
    # import data
    covid_data = os.path.join(input_data, 'covid19_data/')

    df_confirmed = spark.read.format('csv') \
        .options(header='true') \
        .load(covid_data + '/confirmed_data.csv')

    df_deaths = spark.read.format('csv') \
        .options(header='true') \
        .load(covid_data + '/deaths_data.csv')

    df_recovered = spark.read.format('csv') \
        .options(header='true') \
        .load(covid_data + '/recovered_data.csv')

    # filter data
    columns_to_drop = ["Lat", "Long", "Province/State"]
    lat_fr = "46.2276"
    long_fr = "2.2137"

    df_confirmed = df_confirmed.filter(df_confirmed.Lat == lat_fr) \
        .filter(df_confirmed.Long == long_fr) \
        .drop(*columns_to_drop) \
        .withColumnRenamed("Country/Region", "country")

    df_deaths = df_deaths.filter(df_deaths.Lat == lat_fr) \
        .filter(df_deaths.Long == long_fr) \
        .drop(*columns_to_drop) \
        .withColumnRenamed("Country/Region", "country")

    df_recovered = df_recovered.filter(df_recovered.Lat == lat_fr) \
        .filter(df_recovered.Long == long_fr) \
        .drop(*columns_to_drop) \
        .withColumnRenamed("Country/Region", "country")

    # transpose the 3 datasets
    df_confirmed = to_long(df_confirmed, ["country"]).withColumnRenamed("val", "confirmed")
    df_deaths = to_long(df_deaths, ["country"]).withColumnRenamed("val", "deaths")
    df_recovered = to_long(df_recovered, ["country"]).withColumnRenamed("val", "recovered")

    # join the 3 datasets together
    df_covid = df_confirmed.join(df_deaths, ["key", "country"]) \
        .join(df_recovered, ["key", "country"]) \
        .withColumnRenamed("key", "date") \
        .drop("country")

    split_col = F.split(df_covid['date'], '/')
    df_covid = df_covid.withColumn('day', split_col.getItem(1)) \
        .withColumn('month', split_col.getItem(0)) \
        .withColumn('year', split_col.getItem(2))

    # create weather table
    table_covid = df_covid.withColumn('month', F.when(F.length(F.col('month')) == 1,
                                                      F.concat(F.lit('0'), F.col('month'))).otherwise(F.col('month'))) \
        .withColumn('day',
                    F.when(F.length(F.col('day')) == 1, F.concat(F.lit('0'), F.col('day'))).otherwise(F.col('day'))) \
        .withColumn('year',
                    F.when(F.length(F.col('year')) == 2, F.concat(F.lit('20'), F.col('year'))).otherwise(F.col('year'))) \
        .withColumn('date', F.concat('year', F.lit('-'), 'month', F.lit('-'), 'day')) \
        .withColumn("date", F.to_date(F.col("date")))

    # Write weather table
    table_covid.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'covid_table'), mode='overwrite')


def main():
    input_data = sys.argv[1]
    output_data = sys.argv[2]

    spark = create_spark_session()

    process_covid_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

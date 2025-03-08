from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, bround, split, count
from pyspark.sql.window import Window

import csv
import os

# Create the Spark context and Spark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# Entry point for the data files
ENTRY_POINT = "file:///home/user/Downloads/BDAchallenge2425"

# Function to read CSV files
def read_csv(entry_point):
    # Read all CSV files under the given directory path
    df = (
        spark.read.option("header", "true")  # Read with headers
        .csv(entry_point + "/*/*.csv")  # Read all CSV files recursively
        .withColumn("FILENAME", input_file_name())  # Add a column with the filename
        .withColumn("STATION", regexp_extract(input_file_name(), "[^/]+(?=\.csv)", 0))  # Extract station name from filename
        .withColumn("YEAR", regexp_extract(input_file_name(), "/(\d{4})/", 1))  # Extract the year from the file path
    )

    return df

def write_result(df):
    df.show()

# Task 1 - placeholder function
def task1(df):
    result = df.filter(
                (df['LATITUDE'] >= 30) & (df['LATITUDE'] <= 60) &
                (df['LONGITUDE'] >= -135) & (df['LONGITUDE'] <= -90)) \
            .withColumn(
                'TMP', bround(split(df['TMP'], ',')[0].cast('decimal(10, 1)') / 10, 1)) \
            .groupBy('TMP') \
            .agg(count('*').alias('OCC')) \
            .orderBy('OCC', ascending=False) \
            .withColumn('COORDS', lit('[(60,-135);(30,-90)]')) \
            .limit(10) \
            .select('COORDS', 'TMP', 'OCC')

    write_result(result)

# Task 2 - placeholder function
def task2(df):
    windowSpec = Window.partitionBy("SPEED").orderBy(F.desc("OCC"))

    grouped = df.withColumn("SPEED", split(df["WND"], ",").getItem(1)) \
        .groupBy("SPEED", "STATION") \
        .agg(count("*").alias("OCC")) \
        .withColumn("rank", F.rank().over(windowSpec)) \
        .filter(F.col("rank") == 1) \
        .drop("rank") \
        .orderBy("SPEED")

    write_result(grouped)

    # df.withColumn("SPEED", split(df["WND"], ",").getItem(1)) \
    #     .groupBy("SPEED", "STATION") \
    #     .agg(count("*").alias("OCC")) \
    #     .orderBy("SPEED").show()


# Task 3 - placeholder function
def task3(df):
    df.createOrReplaceTempView("temp")
    temp1 = spark.sql("SELECT * FROM temp WHERE REM LIKE '%HOURLY INCREMENTAL PRECIPITATION VALUES (IN)%' ORDER BY REM")
    temp1.show()


# Main block
if __name__ == '__main__':
    # Read the CSV files
    df = read_csv(ENTRY_POINT).select("FILENAME", "STATION", "YEAR", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")

    task1(df)
    task2(df)
    task3(df)

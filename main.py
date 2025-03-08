from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, bround, split, count
from pyspark.sql.window import Window
from pprint import pprint

import csv
import os

# Create the Spark context and Spark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# Entry point for the data files
ENTRY_POINT = "file:///home/user/Downloads/BDAchallenge2425"

from pyspark.sql import functions as F
from functools import reduce


def read_csv(entry_point):
    # Carica i percorsi dei file
    file_paths = spark.sparkContext.wholeTextFiles(entry_point + "/*/*.csv")

    # Usa una lista per raccogliere i DataFrame
    df_list = []

    # Itera sui file senza usare collect()
    for file_path, _ in file_paths.toLocalIterator():
        # Carica i dati in un DataFrame
        df = (
            spark.read.option("header", "true")
            .csv(file_path)
            .withColumn("STATION", F.regexp_extract(F.input_file_name(), "[^/]+(?=\.csv)", 0))
            .withColumn("YEAR", F.regexp_extract(F.input_file_name(), "/(\d{4})/", 1))
            .select("STATION", "YEAR", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")
        )
        # Aggiungi il DataFrame alla lista
        df_list.append(df)

    # Unisci tutti i DataFrame nella lista usando reduce
    final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)

    # Read all CSV files under the given directory path
    # df = (
    #     spark.read.option("header", "true")  # Read with headers
    #     .csv(entry_point + "/*/*.csv")  # Read all CSV files recursively
    #     .withColumn("STATION",
    #                 regexp_extract(input_file_name(), "[^/]+(?=\.csv)", 0))  # Extract station name from filename
    #     .withColumn("YEAR", regexp_extract(input_file_name(), "/(\d{4})/", 1))  # Extract the year from the file path
    # )

    return final_df

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
    temp1 = spark.sql("SELECT * FROM temp WHERE REM LIKE '%HOURLY%' ORDER BY REM DESC")
    temp1.show()

# Main block
if __name__ == '__main__':

    # Read the CSV files
    df = read_csv(ENTRY_POINT)

    task1(df)
    task2(df)
    task3(df)

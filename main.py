from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import aggregate
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql.functions import lit, bround, split, count
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from functools import reduce

import csv
import os

# Create the Spark context and Spark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# Entry point for the data files
ENTRY_POINT = "file:///home/user/Downloads/BDAchallenge2425"

def read_csv(entry_point):
    # Carica i percorsi dei file
    file_paths = spark.sparkContext.wholeTextFiles(entry_point + "/2000/99999994988.csv")

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

# 1. Definire la funzione di somma
def avg_list(values):
    if values:
        return sum(values)/24
    else:
        return 0.00

avg_list_udf = udf(avg_list, DoubleType())

# Task 3 - placeholder function
def task3(df):
    df = df.withColumn(
        "precipitation_values",
        F.regexp_extract(df["REM"], r"HOURLY INCREMENTAL PRECIPITATION VALUES \(IN\):(.*)", 1)
    )

    df = df.withColumn(
        "precipitation_values_list",
        F.expr("FILTER(SPLIT(precipitation_values, '  '), x -> x != '' )")
    )

    df = df.withColumn(
        "precipitation_values_list_numeric",
        F.expr("TRANSFORM(precipitation_values_list, x -> IF(TRIM(x) = 'T', 0.00, CAST(TRIM(x) AS DOUBLE)))")
    )

   #df = df.withColumn(
   #    "precipitation_values_list_numeric",
   #    F.when(
   #        F.size(F.col("precipitation_values_list")) == 0,
   #        F.array(*[F.lit(0.00)])
   #    ).otherwise(
   #        F.expr(
   #            "TRANSFORM(precipitation_values_list, x -> IF(TRIM(x) = 'T', CAST(0.00 AS DOUBLE), CAST(TRIM(x) AS DOUBLE)))"
   #        )
   #    )
   #)

    df = df.withColumn(
        "Average",
        F.when(
            F.size("precipitation_values_list_numeric") == 0,  # Se la lista è vuota
            F.lit(0.0)  # Assegna 0.0
        ).otherwise(
            F.aggregate("precipitation_values_list_numeric", F.lit(0.0), lambda acc, x: acc + x) /
            F.size("precipitation_values_list_numeric")  # Calcola la media
        )
    )

    df.select("Average").show()

# Main block
if __name__ == '__main__':

    # Read the CSV files
    df = read_csv(ENTRY_POINT)

    #task1(df)
    #task2(df)
    task3(df)

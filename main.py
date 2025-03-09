from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import aggregate, lit, bround, split, count, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name, regexp_extract
from functools import reduce
from time import time
import csv
import os

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

ENTRY_POINT = "hdfs://localhost:9000/user/user/BDAchallenge2425"
COLUMNS = ["LATITUDE", "LONGITUDE", "TMP", "WND", "REM"]


def list_file_names(directory_path):
    file_status_objects = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()).listStatus(
        sc._jvm.org.apache.hadoop.fs.Path(directory_path)
    )
    return sorted([str(file.getPath().getName()) for file in file_status_objects])

def read_headers(directory_path):
    headers = {}
    for year in list_file_names(directory_path):
        for station in list_file_names('{}/{}'.format(directory_path, year)):
            path = '{}/{}/{}'.format(directory_path, year, station)
            header = spark.read.option("header", "true").csv(path).columns
            key = tuple(header.index(c) for c in COLUMNS)
            headers[key] = headers.get(key, []) + [(year, station)]
    return headers

def read_csv(csv_files=None):
    return spark.read.format('csv') \
        .option('header', 'true') \
        .load(csv_files) \
        .withColumn("station", F.regexp_extract(F.input_file_name(), "[^/]+(?=\.csv)", 0)) \
        .withColumn("year", F.regexp_extract(F.input_file_name(), "/(\d{4})/", 1))

def get_df(directory_path):

    headers = read_headers(directory_path)

    dfs = []
    for stations in headers.values():
        files = ['{}/{}/{}'.format(directory_path, year, station) for year, station in stations]
        dfs.append(read_csv(files))
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)

    return df


# def read_csv(entry_point):
#     """Read CSV files and process them into a unified DataFrame."""
#     file_paths = spark.sparkContext.wholeTextFiles(entry_point + "/*/*.csv")
#     df_list = []
#
#     for file_path, _ in file_paths.toLocalIterator():
#         df = (
#             spark.read.option("header", "true")
#             .csv(file_path)
#             .withColumn("station", F.regexp_extract(F.input_file_name(), "[^/]+(?=\.csv)", 0))
#             .withColumn("year", F.regexp_extract(F.input_file_name(), "/(\d{4})/", 1))
#             .select("station", "year", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")
#         )
#         df_list.append(df)
#
#     # Merge all the DataFrames from the list
#     final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)
#
#     # Read all CSV files under the given directory path
#     # final_df = (
#     #     spark.read.option("header", "true")  # Read with headers
#     #     .csv(entry_point + "/*/*.csv")  # Read all CSV files recursively
#     #     .withColumn("FILENAME", input_file_name())  # Add a column with the filename
#     #     .withColumn("station",
#     #                 regexp_extract(input_file_name(), "[^/]+(?=\.csv)", 0))  # Extract station name from filename
#     #     .withColumn("year", regexp_extract(input_file_name(), "/(\d{4})/", 1))  # Extract the year from the file path
#     # )
#
#     return final_df


def write_result(df):
    """Display the result."""
    df.show()


def task1(df):
    """Task 1: Filter by coordinates, process TMP column, and show the top 10 results."""
    result = (
        df.filter(
            (df["LATITUDE"] >= 30) & (df["LATITUDE"] <= 60) &
            (df["LONGITUDE"] >= -135) & (df["LONGITUDE"] <= -90)
        )
        .withColumn(
            "TMP", bround(split(df["TMP"], ",")[0].cast("decimal(10, 1)") / 10, 1)
        )
        .groupBy("TMP")
        .agg(count("*").alias("OCC"))
        .orderBy("OCC", ascending=False)
        .withColumn("COORDS", lit("[(60,-135);(30,-90)]"))
        .limit(10)
        .select("COORDS", "TMP", "OCC")
    )

    write_result(result)

    # Save the result into CVS file
    # output_path = "file:///home/user/Downloads/task1.csv"
    # try:
    #    result.select("COORDS", "TMP", "OCC") \
    #        .coalesce(1) \
    #    .write \
    #    .mode("overwrite") \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")


def task2(df):
    """Task 2: Find the most frequent station per wind speed."""
    window_spec = Window.partitionBy("SPEED").orderBy(F.desc("OCC"))

    grouped = (
        df.withColumn("SPEED", split(df["WND"], ",").getItem(1))
        .groupBy("SPEED", "station")
        .agg(count("*").alias("OCC"))
        .withColumn("rank", F.rank().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .orderBy("SPEED")
    )

    write_result(grouped)

    # Save the result into CVS file
    # output_path = "file:///home/user/Downloads/task2.csv"
    # try:
    #    grouped.select("SPEED", "station", "OCC") \
    #        .coalesce(1) \
    #    .write \
    #    .mode("overwrite") \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")

    # Old code
    # df.withColumn("SPEED", split(df["WND"], ",").getItem(1)) \
    #     .groupBy("SPEED", "station") \
    #     .agg(count("*").alias("OCC")) \
    #     .orderBy("SPEED").show()


def task3(df):
    """Task 3: Process precipitation values and calculate the average per station per year."""

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

    df = df.withColumn(
        "Average",
        F.when(
            F.size("precipitation_values_list_numeric") == 0,
            F.lit(0.0)
        ).otherwise(
            F.aggregate("precipitation_values_list_numeric", F.lit(0.0), lambda acc, x: acc + x) /
            F.size("precipitation_values_list_numeric")
        )
    )

    df_media_precipitazioni = df.groupBy("year", "station").agg(
        F.avg("Average").alias("avg_precipitation")
    )

    df_ordinato = df_media_precipitazioni.orderBy("year", "avg_precipitation")

    finestra_spec = Window.partitionBy("year").orderBy("avg_precipitation")

    df_top_10_stazioni = (
        df_ordinato.withColumn("rank", F.row_number().over(finestra_spec))
        .filter(F.col("rank") <= 10)
        .drop("rank")
    )

    write_result(df_top_10_stazioni)

    # Save the result into CVS file
    # output_path = "file:///home/user/Downloads/task3.csv"
    # try:
    #    df_top_10_stazioni.select("year", "station", "avg_precipitation") \
    #        .coalesce(1) \
    #    .write \
    #    .mode("overwrite") \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")


# Main block
if __name__ == '__main__':

    df = get_df(ENTRY_POINT)

    task1(df)
    task2(df)
    task3(df)


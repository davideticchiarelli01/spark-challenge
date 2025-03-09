from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import aggregate, lit, bround, split, count
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from functools import reduce
import time
import csv
import os

# Create the Spark context and Spark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# Entry point for the data files
ENTRY_POINT = "file:///home/user/Downloads/BDAchallenge2425"


def read_csv(entry_point):
    """Read CSV files and process them into a unified DataFrame."""
    file_paths = spark.sparkContext.wholeTextFiles(entry_point + "/*/*.csv")
    df_list = []

    for file_path, _ in file_paths.toLocalIterator():
        df = (
            spark.read.option("header", "true")
            .csv(file_path)
            .withColumn("STATION", F.regexp_extract(F.input_file_name(), "[^/]+(?=\.csv)", 0))
            .withColumn("YEAR", F.regexp_extract(F.input_file_name(), "/(\d{4})/", 1))
            .select("STATION", "YEAR", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")
        )
        df_list.append(df)

    # Merge all the DataFrames from the list
    final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)

    return final_df


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
        .groupBy("SPEED", "STATION")
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
    #    grouped.select("SPEED", "STATION", "OCC") \
    #        .coalesce(1) \
    #    .write \
    #    .mode("overwrite") \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")

    # Old code
    # df.withColumn("SPEED", split(df["WND"], ",").getItem(1)) \
    #     .groupBy("SPEED", "STATION") \
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

    df_media_precipitazioni = df.groupBy("YEAR", "STATION").agg(
        F.avg("Average").alias("avg_precipitation")
    )

    df_ordinato = df_media_precipitazioni.orderBy("YEAR", "avg_precipitation")

    finestra_spec = Window.partitionBy("YEAR").orderBy("avg_precipitation")

    df_top_10_stazioni = (
        df_ordinato.withColumn("rank", F.row_number().over(finestra_spec))
        .filter(F.col("rank") <= 10)
        .drop("rank")
    )

    write_result(df_top_10_stazioni)

    # Save the result into CVS file
    # output_path = "file:///home/user/Downloads/task3.csv"
    # try:
    #    df_top_10_stazioni.select("YEAR", "STATION", "avg_precipitation") \
    #        .coalesce(1) \
    #    .write \
    #    .mode("overwrite") \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")


# Main block
if __name__ == '__main__':
    start_time = time.time()
    # Read all CSV files
    df = read_csv(ENTRY_POINT)

    task1(df)
    task2(df)
    task3(df)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("All operations have terminated in {:.2f} s.".format(elapsed_time))

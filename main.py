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
    # Load paths of files
    file_paths = spark.sparkContext.wholeTextFiles(entry_point + "/*/*.csv")
    df_list = []

    # Iterate over files
    for file_path, _ in file_paths.toLocalIterator():
        # Load the data into a DataFrame
        df = (
            spark.read.option("header", "true")
            .csv(file_path)
            .withColumn("STATION", F.regexp_extract(F.input_file_name(), "[^/]+(?=\.csv)", 0))
            .withColumn("YEAR", F.regexp_extract(F.input_file_name(), "/(\d{4})/", 1))
            .select("STATION", "YEAR", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")
        )
        #Add the DataFrame into a list
        df_list.append(df)

    # Merge all the Dataframes from the list
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

    # Save the result into CVS file
    # output_path = "file:///home/user/Downloads/task1.csv"  # Modifica il percorso del file di output
    ## Scrivi il DataFrame in un file CSV senza sovrascrivere
    # try:
    #    result.select("COORDS", "TMP", "OCC") \
    #        .coalesce(1) \
    #    .write \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")




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

    # Save the result into CVS file
    # output_path = "file:///home/user/Downloads/task2.csv"  # Modifica il percorso del file di output
    ## Scrivi il DataFrame in un file CSV senza sovrascrivere
    # try:
    #    grouped.select("SPEED", "STATION", "OCC") \
    #        .coalesce(1) \
    #    .write \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    # except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")


    # df.withColumn("SPEED", split(df["WND"], ",").getItem(1)) \
    #     .groupBy("SPEED", "STATION") \
    #     .agg(count("*").alias("OCC")) \
    #     .orderBy("SPEED").show()

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

    # Step 2: Ordina per anno e precipitazione media in ordine crescente
    df_ordinato = df_media_precipitazioni.orderBy("YEAR", "avg_precipitation")

    # Step 3: Usa le funzioni di finestra per ottenere le prime 10 stazioni per ogni anno
    finestraSpec = Window.partitionBy("YEAR").orderBy("avg_precipitation")

    df_top_10_stazioni = df_ordinato.withColumn("rank", F.row_number().over(finestraSpec)) \
        .filter(F.col("rank") <= 10)  # Seleziona solo le prime 10 stazioni per anno

    df_top_10_stazioni = df_top_10_stazioni.drop("rank")

    write_result(df_top_10_stazioni)

    # Save the result into CVS file
    #output_path = "file:///home/user/Downloads/task3.csv"  # Modifica il percorso del file di output
    ## Scrivi il DataFrame in un file CSV senza sovrascrivere
    #try:
    #    df_top_10_stazioni.select("YEAR", "STATION", "avg_precipitation") \
    #        .coalesce(1) \
    #    .write \
    #        .csv(output_path, header=True)
    #    print(f"File salvato con successo in {output_path}")
    #except Exception as e:
    #    print(f"Errore nel salvataggio del file: {e}")

# Main block
if __name__ == '__main__':
    start_time = time.time()
    # Read the CSV files
    df = read_csv(ENTRY_POINT)

    #task1(df)
    #task2(df)
    task3(df)
    end_time = time.time()  # Registra il tempo di fine
    elapsed_time = end_time - start_time  # Calcola il tempo trascorso
    print("All operations have terminated in {:.2f} s.".format(elapsed_time))
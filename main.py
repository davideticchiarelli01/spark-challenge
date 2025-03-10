from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import aggregate, lit, bround, split, count, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name, regexp_extract
from functools import reduce
import time
import csv

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#ENTRY_POINT = "file:///home/user/Downloads/BDAchallenge2425"
ENTRY_POINT = "hdfs://localhost:9000/user/user/BDAchallenge2425"
OUTPUT_PATH = "/home/user/Downloads"
#OUTPUT_PATH = "/home/amircoli/Scrivania/BDA/spark2425/results/gruppo_3"
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

    return df.select(["station","year"] + COLUMNS)


#def read_csv(entry_point):
#    """Read CSV files and process them into a unified DataFrame."""
#    file_paths = spark.sparkContext.wholeTextFiles(entry_point + "/*/*.csv")
#    df_list = []
#
#    for file_path, _ in file_paths.toLocalIterator():
#        df = (
#            spark.read.option("header", "true")
#            .csv(file_path)
#            .withColumn("station", F.regexp_extract(F.input_file_name(), "[^/]+(?=\.csv)", 0))
#            .withColumn("year", F.regexp_extract(F.input_file_name(), "/(\d{4})/", 1))
#            .select("station", "year", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")
#        )
#        df_list.append(df)
#
#    # Merge all the DataFrames from the list
#    final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)
#
#    # Read all CSV files under the given directory path
#    # final_df = (
#    #     spark.read.option("header", "true")  # Read with headers
#    #     .csv(entry_point + "/*/*.csv")  # Read all CSV files recursively
#    #     .withColumn("FILENAME", input_file_name())  # Add a column with the filename
#    #     .withColumn("station",
#    #                 regexp_extract(input_file_name(), "[^/]+(?=\.csv)", 0))  # Extract station name from filename
#    #     .withColumn("year", regexp_extract(input_file_name(), "/(\d{4})/", 1))  # Extract the year from the file path
#    # )
#
#    return final_df


def write_result(df):
    """Display the result."""

    df.show()


def save_to_csv(df, filename):
    """Save a DataFrame into a CSV file."""
    try:
        df.write \
            .format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .save('file://{}/{}'.format(OUTPUT_PATH, filename))
        print(f"File salvato con successo in {OUTPUT_PATH}")
    except Exception as e:
        print(f"Errore nel salvataggio del file {filename}: {e}")


def task1(df):
    """Task 1: Filter by coordinates, process TMP column, and show the top 10 results."""

    start_time = time.time()

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

    #write_result(result)

    save_to_csv(result, "task1")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("All operations for the task1 have terminated in {:.2f} s.".format(elapsed_time))



def task2(df):
    """Task 2: Find the most frequent station per wind speed."""

    start_time = time.time()

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

    #write_result(grouped)

    save_to_csv(grouped, "task2")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("All operations for the task2 have terminated in {:.2f} s.".format(elapsed_time))


def task3(df):
    """Task 3: Process precipitation values and calculate the average per station per year."""

    start_time = time.time()

    df = df.filter(F.col("REM").contains("HOURLY INCREMENTAL PRECIPITATION VALUES (IN):"))

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
            F.aggregate("precipitation_values_list_numeric", F.lit(0.0), lambda acc, x: acc + x) /F.size("precipitation_values_list_numeric")
    )

    df_media_precipitazioni = df.groupBy("year", "station").agg(
        F.avg("Average").alias("avg_precipitation")
    )

    df_ordinato = df_media_precipitazioni.orderBy("year", "avg_precipitation")

    finestra_spec = Window.partitionBy("year").orderBy("avg_precipitation")

    df_top_10_stazioni = (
        df_ordinato.withColumn("rank", F.dense_rank().over(finestra_spec))
        .filter(F.col("rank") <= 10)
        .drop("rank")
        .limit(10)
    )

    #write_result(df_top_10_stazioni)

    save_to_csv(df_top_10_stazioni, "task3")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("All operations for the task3 have terminated in {:.2f} s.".format(elapsed_time))


# Main block
if __name__ == '__main__':

    df = get_df(ENTRY_POINT)

    #df = read_csv(ENTRY_POINT)

    task1(df)
    task2(df)
    task3(df)


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name, regexp_extract, split
import csv
import os

# Create the Spark context and Spark session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# Entry point for the data files
ENTRY_POINT = "file:///home/user/Downloads//BDAchallenge2425"

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

# Task 1 - placeholder function
def task1(df):
    return 'ciao'

# Task 2 - placeholder function
def task2(df):
    return 'ciao'

# Task 3 - placeholder function
def task3(df):
    return 'ciao'

# Main block
if __name__ == '__main__':
    # Read the CSV files
    df = read_csv(ENTRY_POINT)

    # Select specific columns: FILENAME, STATION, YEAR, LATITUDE, LONGITUDE, TMP, WND, REM
    df_selected = df.select("FILENAME", "STATION", "YEAR", "LATITUDE", "LONGITUDE", "TMP", "WND", "REM")


    # Count the total number of rows in the selected dataframe
    row_count = df_selected.count()
    print(f"Total number of rows: {row_count}")  # Print the number of rows
    #edfefef

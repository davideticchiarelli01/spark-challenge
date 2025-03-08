from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# Creazione del contesto Spark e della sessione Spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def print_hi(name):
    print(f'Hi, {name}')  # Messaggio di benvenuto


if __name__ == '__main__':
    text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
    text.collect()
    
    # Chiude il contesto Spark
    spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count
from time import sleep

spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .master("local[*]") \
    .getOrCreate()

full_file = "app/data/full.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiline", "true") \
    .load(full_file, format="csv")

# 1 - Les 10 projets Github pour lesquels il y a eu le plus de commit
top_projects = df.groupBy("repo").count() \
                 .orderBy("count", ascending=False).limit(10)
print("Les 10 projets Github pour lesquels il y a eu le plus de commit :")
top_projects.show()

# 2 - Le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark
spark_contributors = df.filter("repo = 'apache/spark'") \
                      .groupBy("author").count() \
                      .orderBy("count", ascending=False)
top_contributor = spark_contributors.first()
print("Le plus gros contributeur du projet apache/spark :")
print(f"{top_contributor['author']}: {top_contributor['count']} commits")

sleep(1000)



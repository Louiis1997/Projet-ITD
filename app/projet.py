from pyspark.sql import SparkSession
from pyspark.sql.functions import F
from time import sleep

spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 6) \
    .getOrCreate()

full_file = "/app/data/full.csv"

# 1
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(full_file, format="csv") \
    .repartition(10)

# # 2
mnm_df = mnm_df \
    .groupBy("State") \
    .agg(sum("Count").alias("Total")) \
    .orderBy("Total", ascending=False) \

mnm_df.show(n=10, truncate=False)

# 5
mnm_df.cache()

for i in range(10):
    print(mnm_df.count())

sleep(1000)



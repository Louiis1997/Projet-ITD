from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, to_date, date_sub, current_date, date_format
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

# 3. Les plus gros contributeurs du projet apache/spark sur les 4 dernières années
four_years_ago = date_sub(current_date(), 365*4)
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn("date", to_date(df["date"], "EEE MMM dd HH:mm:ss yyyy Z"))
df = df.withColumn("date_formatted", date_format(df["date"], "dd/MM/yyyy"))
recent_spark_contributors = df.filter((df.repo == "apache/spark") & (df.date >= four_years_ago)) \
                             .groupBy("author").count() \
                             .orderBy("count", ascending=False).limit(10)
print("Les plus gros contributeurs du projet apache/spark sur les 4 dernières années :")
recent_spark_contributors.show(truncate=False)

import os
from pyspark.sql.functions import desc
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import col, lower, regexp_replace, split, explode, length, array
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
top_projects.show(truncate=False)


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


# 4. Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets. Vous prendrez soin d’éliminer de la liste les stopwords pour ne pas les prendre en compte. Vous êtes libre d’utiliser votre propre liste de stopwords, vous pouvez sinon trouver des listes ici.

# Process commit messages: split into words, lower case, remove punctuation
words = df.select(explode(split(lower(regexp_replace(
    col('message'), '[^\w\s]', ' ')), ' ')).alias('word'))

# Remove empty strings and non-alphabetic "words"
words = words.filter((length(col('word')) > 0) & (
    col('word').rlike('[a-z]'))).withColumn('word', trim(col('word')))

# Convert each row into a list of words
words = words.withColumn('word', array('word'))
print("Filtered words with punctuation removed and empty strings removed")
print(words)

# Directory containing the stopword text files
stopwords_dir = './data/stop_words/'

# Read all stopword files and combine into a single list
stopwords_list = []
for filename in os.listdir(stopwords_dir):
    if filename.endswith('.txt'):
        with open(os.path.join(stopwords_dir, filename), 'r', encoding='ISO-8859-1') as f:
            stopwords_list += [word.strip() for word in f.readlines()]
print("Stop words list")
print(stopwords_list)

# Use the loaded stop words list
remover = StopWordsRemover(stopWords=stopwords_list)
remover.setInputCol('word')
remover.setOutputCol('filtered')

# Remove stop words
words_no_stop_words = remover.transform(words)

# Make column "filtered" as string
words_no_stop_words = words_no_stop_words.withColumn(
    'filtered', explode('filtered'))

# Count word frequencies and show the top 10
words_no_stop_words.groupBy('filtered').count().orderBy(desc('count')).show(10)

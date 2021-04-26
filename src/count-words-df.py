from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDf = spark.read.text("c:/repositorios/Spark-Course/data/book/Book.txt")

words = inputDf.select(func.explode(func.split(inputDf.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(wordCountsSorted.count())


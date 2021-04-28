from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(spark, data):
    pairScores = data.withColumn("xx", func.col("rating1") * func.col("rating1")).withColumn("yy", func.col("rating2") * func.col("rating2")).withColumn("xy", func.col("rating1") * func.col("rating2"))
    
    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(func.sum(func.col("xy")).alias("numerator"), (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), func.count(func.col("xy")).alias("numPairs"))

    result = calculateSimilarity.withColumn("score", func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")).otherwise(0)).select("movie1", "movie2", "score", "numPairs")

    return result

def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieId") == movieId).select("movieTitle").collect()[0]
    return result[0]

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([StructField("movieId", IntegerType(), True),StructField("movieTitle", StringType(), True)])

moviesSchema = StructType([StructField("userId", IntegerType(), True),StructField("movieId", IntegerType(), True),StructField("rating", IntegerType(), True),StructField("timestamp", LongType(), True) ])

movieNames = spark.read.option("sep", "|").option("charset", "ISO-8859-1").schema(movieNamesSchema).csv("C:/repositorios/Spark-Course/data/ml-100k/u.item")

movies = spark.read.option("sep", "\t").schema(moviesSchema).csv("C:/repositorios/Spark-Course/data/ml-100k/u.data")

ratings = movies.select("userId", "movieId", "rating")

moviePairs = ratings.alias("ratings1").join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))).select(func.col("ratings1.movieId").alias("movie1"), func.col("ratings2.movieId").alias("movie2"), func.col("ratings1.rating").alias("rating1"), func.col("ratings2.rating").alias("rating2"))
moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOcurrenceThreshold = 50.0

    movieId = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(((func.col("movie1") == movieId) | (func.col("movie2") == movieId)) & (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOcurrenceThreshold))

    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieId)) 

    for result in results:
        similarMovieId = result.movie1
        if (similarMovieId == movieId):
            similarMovieId = result.movie2
        
        print(getMovieName(movieNames, similarMovieId) + "\tscore: " + str(result.score) + "\tstrength: " + str(result.numPairs))
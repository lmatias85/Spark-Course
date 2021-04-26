from pyspark.sql import SparkSession 
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs 

def loadMoviesNames():
    movieNames = {}
    with codecs.open("C:/repositorios/Spark-Course/data/ml-100k/u.item","r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def lookupName(movieId):
    return nameDict.value[movieId]

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMoviesNames())

schema = StructType([
                     StructField("userId", IntegerType(), True),
                     StructField("movieId", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)
                    ])        

moviesDf = spark.read.option("sep", "\t").schema(schema).csv("C:/repositorios/Spark-Course/data/ml-100k/u.data")

movieCounts = moviesDf.groupBy("movieId").count()

lookupNameUDF = func.udf(lookupName)

moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieId")))

sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()
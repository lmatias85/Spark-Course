from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS 
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("C:/repositorios/Spark-Course/data/ml-100k/u.item", "r", encoding = 'ISO-8859-1', errors = 'ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("ALSExample").getOrCreate()

moviesSchema = StructType([ \
                        StructField("userId", IntegerType(), True), \
                        StructField("movieId", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestamp", LongType(), True)])

names = loadMovieNames()

ratings = spark.read.option("sep", "\t").schema(moviesSchema).csv("C:/repositorios/Spark-Course/data/ml-100k/u.data")

print("Training recommendation model...")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

model = als.fit(ratings)

# Manually construct a dataframe of the user ID's we want recs for
userId = int(sys.argv[1])
userSchema = StructType([StructField("userId", IntegerType(), True)])
users = spark.createDataFrame([[userId,]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(userId))

for userRecs in recommendations:
    myRecs = userRecs[1]  #userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in myRecs: #my Recs is just the column of recs for the user
        movie = rec[0] #For each rec in the list, extract the movie ID and rating
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))
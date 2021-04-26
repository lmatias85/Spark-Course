from pyspark.sql import SparkSession as ss
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = ss.builder.appName("PopularMovies").getOrCreate()

schema = StructType([StructField("userId", IntegerType(), True),
                     StructField("movieId", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)
                    ])

moviesDf = spark.read.option("sep", "\t").schema(schema).csv("C:/repositorios/Spark-Course/data/ml-100k/u.data")

topMovieIds = moviesDf.groupBy("movieId").count().orderBy(func.desc("count"))

topMovieIds.show(10)

spark.stop()
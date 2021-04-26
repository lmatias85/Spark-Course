from pyspark.sql import SparkSession as ss
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = ss.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true").csv("C:/repositorios/Spark-Course/data/fake-friends/fakefriends_2.csv")

print("Average number of friends by age: ")

friendsByAge = people.select("age", "friends")

friendsByAge.groupBy("age").avg("friends").show()

friendsByAge.groupBy("age").avg("friends").sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()
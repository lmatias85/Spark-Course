from pyspark.sql import SparkSession as ss

spark = ss.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true").csv("C:/repositorios/Spark-Course/data/fake-friends/fakefriends_2.csv")

print("Here is our inferred schema: ")
people.printSchema()

print("Let's display the name column: ")
people.select("name").show()

print("Filter out anyone over 21")
people.filter(people.age < 21).show()

print("Group by age:")
people.groupBy("age").count().show()

print("Make everyone 10 years older: ")
people.select(people.name, people.age + 10).show()

spark.stop()




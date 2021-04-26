from pyspark.sql import SparkSession as ss
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = ss.builder.appName("SpentCustomer").getOrCreate()

schema = StructType([StructField("customerId", IntegerType(), True),
                     StructField("itemId", IntegerType(), True),
                     StructField("amount", FloatType(), True)])

df = spark.read.schema(schema).csv("c:/repositorios/Spark-Course/data/customer-orders/customer-orders.csv")
df.printSchema()

spentByCustomer = df.groupBy("customerId").agg(func.round(func.sum("amount"),2).alias("totalSpent"))
spentByCustomerSorted = spentByCustomer.sort("totalSpent")

spentByCustomerSorted.show(spentByCustomerSorted.count())

spar.stop()
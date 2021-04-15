from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(",")
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer, amount)

conf = SparkConf().setMaster("local").setAppName("TotalByCustomer")
sc = SparkContext(conf = conf)

line = sc.textFile("c:/repositorios/Spark-Course/data/customer-orders/customer-orders.csv")
parsedLines = line.map(parseLine)

totalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y)
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = totalByCustomerSorted.collect()

for result in results:
    totalAmount = str(result[0])
    customer = str(result[1])
    print("Customer: " + customer + "\t\t" + "Total Amount: " + totalAmount)




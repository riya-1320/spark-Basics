from pyspark import SparkContext, SparkConf

# spark context
conf = SparkConf().setAppName("RDD_APP").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# rdd via external database
RddExternalDb = sc.textFile("/home/xs354-riygup/TASKS/spark-task/data/sparkData.txt")
print('RDD Via external Database: ')
print(RddExternalDb .collect())

# rdd via parallelize method
data = [2, 6, 10, 5, 3]
RddParallelize = sc.parallelize(data)
print('RDD Via parallelize: ')
print(RddParallelize.collect())

# Map operation: Square each element of the RDD
squaredRdd = RddParallelize.map(lambda x: x ** 2)
print('Square of RDD: ')
print(squaredRdd.collect())

# Filter operation: Select only even numbers from the RDD
evenRdd = squaredRdd.filter(lambda x: x % 2 == 0)
print('Even number from RDD: ')
print(evenRdd.collect())

# Reduce operation: Sum all the elements in the RDD
sumOfElements = evenRdd.reduce(lambda x, y: x + y)

# Print the final result
print('Sum of even squared numbers:', sumOfElements)
sc.stop()

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD_APP").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

linesRdd = sc.textFile("/home/xs354-riygup/TASKS/spark-task/data/sparkData.txt")

wordCountsRdd = linesRdd.flatMap(lambda x: x.split(" ")).map(lambda term: (term, 1)).reduceByKey(lambda a, b: a + b)

# Collect the result
result = wordCountsRdd.collect()

# Print the term count
for word, count in result:
    print(word, count)

# Stop the SparkContext
sc.stop()




















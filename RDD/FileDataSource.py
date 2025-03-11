from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

fileRdd = sc.textFile("data/data.txt")
print(fileRdd.collect())
print(fileRdd.count());
print(fileRdd.first());
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
print(numbers.take(7))
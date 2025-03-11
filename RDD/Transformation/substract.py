from pyspark import SparkContext, SparkConf
from random import Random
import time
conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)
text = sc.parallelize(["Dit me thang Dat noi ngu vai loz chu may a"])\
        .flatMap(lambda i:i.split(" "))\
        .map(lambda i: i.lower())
removeText = sc.parallelize(["dit me vai lon"])\
            .flatMap(lambda i:i.split(" "))\
            .map(lambda i: i.lower())
niceText = text.subtract(removeText)
print(text.collect())
print(niceText.collect())
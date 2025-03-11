from pyspark import SparkContext, SparkConf
from random import Random
import time
conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([1,123,3,5,11,8])
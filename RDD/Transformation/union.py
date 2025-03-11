from pyspark import SparkContext, SparkConf
from random import Random
import time
conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

rdd1 = sc.parallelize([1,2,3,4,5,6])
rdd2 = sc.parallelize([1,34,5241,12,1])
rdd3 = rdd1.union(rdd2)
print(rdd3.collect())
rdd4 =sc.parallelize ([
    {"id": 1, "name": "daat"},
    {"id": 1, "name": "daat"},
    {"id": 1, "name": "daat"}
])
print(rdd4.union(rdd3).collect())
from pyspark import SparkContext, SparkConf
from random import Random
import time
conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

valueRdd = sc.parallelize(["one",1,2,"three","two",4,1,"ome"])
print(valueRdd.distinct().collect())
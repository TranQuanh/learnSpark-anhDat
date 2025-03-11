from pyspark import SparkContext, SparkConf
from random import Random
import time
conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

data = ["Dat","Golden","Hieu kkk","sami"]
rdd = sc.parallelize(data)
def numPartition(iterator):
    rand = Random(int(time.time()*1000) +Random().randint(0,1000))
    return[f"{item}:{rand.randint(0,1000)}" for item in iterator]
result = rdd.mapPartitions(numPartition)
print(result.collect())
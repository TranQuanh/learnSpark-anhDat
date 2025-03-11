from pyspark import SparkContext, SparkConf
from random import Random
import time
conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

"""
idx: id of partition(ex: 0,1,2,3,....)
iterator: vong lap qua tat ca phan tu qua phan vung

lambda
- idx: chi so cuar partition
- irt: vong lap qua cac phan trong phan vung
"""
numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],13)
print(numbersRdd.glom().collect())
result = (numbersRdd.mapPartitionsWithIndex(
    lambda idx,itr:[(idx,n) for n in itr]
).collect())
print(result)


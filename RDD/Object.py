from pyspark import SparkContext

sc = SparkContext("local","Quanhdz");

data = [
    {"id":1,"name":"daat"},
    {"id":1,"name":"daat"},
    {"id":1,"name":"daat"}
]
rdd = sc.parallelize(data);
print(rdd.collect());
print(rdd.count());
print(rdd.first());

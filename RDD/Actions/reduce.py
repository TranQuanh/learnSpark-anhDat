from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)


numberRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10,11,12],
                           3)
print(numberRdd.glom().collect())
def sum(v1:int,v2:int) -> int:
    print(f"v1 : {v1}, v2: {v2}=> ({v1+v2})")
    return v1+v2

print(numberRdd.reduce(sum))
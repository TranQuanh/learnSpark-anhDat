from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("QUanh dep zai").setMaster("local[*]").set("spark.excecute.memory","4g")

sc = SparkContext(conf=conf)
number = [1,2,3,4,5,6,7,8]
rdd = sc.parallelize(number)
squareRdd = rdd.map(lambda i:i+1)
filterRdd = rdd.filter(lambda i: i>4)
flatmapRdd = rdd.flatMap(lambda i :[i,i*2])
print(rdd.collect())
print(squareRdd.collect())
print(filterRdd.collect())
print(flatmapRdd.collect())
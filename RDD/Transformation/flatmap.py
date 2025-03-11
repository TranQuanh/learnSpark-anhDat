from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quanhdz").setMaster("local[*]").set("spark.executor.memory","4g")
sc = SparkContext(conf = conf)

fileRdd = sc.textFile("../data/data.txt")
print(fileRdd.collect())

flatmapRdd = fileRdd.flatMap(lambda i: i.split(" "))
for line in flatmapRdd.collect():
    print(line,sep=" ",end="")
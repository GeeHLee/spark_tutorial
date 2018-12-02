# a first spark program: word count
from pyspark import SparkContext
sc = SparkContext("local", "test")
# search the local file
text_file = sc.textFile("data/words.txt")
# split the whole file by space.
flat_words = text_file.flatMap(lambda line: line.split(" "))
# the code below will create a tuple (word, 1) with key = word and value = 1 for each word in the flat_words.
tuple_words = flat_words.map(lambda word:(word, 1))
# then reduce by key
word_count = tuple_words.reduceByKey(lambda a, b: a + b)
word_count.collect()

#%% create a RDD object:
# from local file:
text_file = sc.textFile("data/words.txt")
# create by a array (since python don't have array so here we use a list):
nums = [1, 2, 3, 4, 5]
rdd = sc.parallelize(nums)
# when a RDD have been created, there is two operation possibles:
# Transformation
# Action
# here is some example for transformation:
trans_filter = rdd.filter(lambda ele: ele > 2).collect()
print(trans_filter)

trans_map = rdd.map(lambda ele: ele * 2).collect()
print(trans_map)

trans_flatmap = rdd.flatMap(lambda ele: range(ele, 5)).collect()
print(trans_flatmap)
# unlike map the flatmap will flat outputs to one object after mapping.

# here is some example for action:
action_count = rdd.count()
print(action_count)

action_collect = rdd.collect()
print(action_collect)

action_first = rdd.first()
print(action_first)

action_take = rdd.take(3)
print(action_take)

action_reduce = rdd.reduce(lambda a, b: a if a > b else b)
print(action_reduce)

# cache:
# since rdd will not execute (charge in the cache) until a given action.
# so if we need more then one action, spark will start over on each time.
# Therefore the solution is charge the RDD in the cache manually.

list = ["Hadoop", "Spark", "Hive"]
rdd = sc.parallelize(list)
rdd.cache()
print(rdd.count())
print(', '.join(rdd.collect()))
rdd.unpersist() # delete this RDD in the cache.

# partition
# we can set spark.default.parallelism to making number of partition.
# in local mode: it's the number of cpu core by default.
# in Mesos: it's 8.
# in Standalone or YARN: max(2, all cpu core in the clusters)
list = [1,2,3,4,5]
rdd = sc.parallelize(list, 2)

#%% key value RDD:
# create pair RDD from local file.
lines = sc.textFile("data/words.txt")
pair_rdd = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
pair_rdd.foreach(print)
# create pair RDD from list.
list = ["Hadoop", "Spark", "Hive", "Spark"]
rdd = sc.parallelize(list)
rdd.cache() # since following example will use this rdd.
pair_rdd = rdd.map(lambda word: (word, 1))
pair_rdd.foreach(print)

# common transformation operation for pair RDD:
# reduceByKey: reduce value by keys using costume functions.
pair_rdd.reduceByKey(lambda a, b: a + b).foreach(print)
# groupByKey: group by key in KV RDD.
pair_rdd.groupByKey().foreach(print)
# keys: create a new RDD which contain only the keys.
pair_rdd.keys().foreach(print)
# values: create a new RDD which contain only the values.
pair_rdd.values().foreach(print)
# sortByKey: sort a KV RDD by keys.
pair_rdd.sortByKey().foreach(print)
# mapValues: operation only affect the values.
pair_rdd.mapValues(lambda x: x * 2).foreach(print)
# join: include join, leftOuterJoin, rightOuterJoin ...
pair_rdd1 = sc.parallelize([('spark', 1), ('spark', 2), ('hadoop', 3), ('hadoop', 5)])
pair_rdd2 = sc.parallelize([('spark', 'fast')])
pair_rdd1.leftOuterJoin(pair_rdd2).foreach(print)
pair_rdd1.join(pair_rdd2).foreach(print)

# example: calculate the average volume of a KV RDD given.
rdd = sc.parallelize([("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)])
# mapValues: create a count value in tuple.
# reduceByKey: 0 index for volume, 1 index for count
# second mapValues: calculation of average.
rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: (x[0]/x[1])).collect()

#%% broadcast variables:

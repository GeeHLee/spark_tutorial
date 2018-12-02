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


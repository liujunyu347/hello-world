from pyspark import SparkContext
from pyspark import SparkConf
from sys import argv
import json
import time
def swap(x):
    return (x[1], x[0])
def bussiness_id(line):
    return line[2][15:-1]
def add(a,b):
    return a+b
def countf(iterator):
    count = len(list(iterator))
    yield count

input_file = argv[1]
output_file = open(argv[2], "w", encoding='utf-8')
n_partition = argv[3]
sc = SparkContext("local[*]")
sf = SparkConf()
sf.set("spark.executor.memory", "4g")
review = sc.textFile(input_file)
output = dict()
review_strip = review.map(lambda s: s.strip("{}"))
review_split = review_strip.map(lambda s: s.split(","))

default = dict()
default["n_partition"] = review_split.map(bussiness_id).map(lambda x: [x, 1]).reduceByKey(add).sortByKey(True).map(swap).sortByKey(False).map(swap).getNumPartitions()
default["n_items"] = review_split.mapPartitions(countf).collect()
start_time = time.time()
execution = review_split.map(bussiness_id).map(lambda x: [x, 1]).reduceByKey(add).sortByKey(True).map(swap).sortByKey(False).map(swap).take(10)
end_time = time.time()
default["exe_time"] = end_time - start_time

customized = dict()
review_split = review_split.repartition(int(n_partition))
customized["n_partition"] = review_split.getNumPartitions()
customized["n_items"] = review_split.mapPartitions(countf).collect()
start_time = time.time()
execution = review_split.map(bussiness_id).map(lambda x: [x, 1]).reduceByKey(add).sortByKey(True).map(swap).sortByKey(False).map(swap).take(10)
end_time = time.time()
customized["exe_time"] = end_time - start_time

# print(default)
# print(customized)
explanation = "More partitions will increase the shuffling time resulting more running time." \
              " If we reduce the number of partition, it will reduce the shuffling time resulting less running time."

output["default"] = default
output["customized"] = customized
output["explanation"] = explanation

#with open('task2.json', 'w') as outfile:
json.dump(output, output_file)
output_file.close()
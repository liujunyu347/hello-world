from pyspark import SparkContext
from pyspark import SparkConf
from sys import argv
import json

input_file = argv[1]
output_file = open(argv[2], "w", encoding='utf-8')

def date(line):
    if "2018" in line[-1]:
        return 1
def user_id(line):
    return line[1][11:-1]
def bussiness_id(line):
    return line[2][15:-1]
def add(a,b):
    return a+b
def swap(x):
    return (x[1], x[0])

sc = SparkContext("local[*]")
sf = SparkConf()
sf.set("spark.executor.memory", "4g")
review = sc.textFile(input_file).coalesce(12)

output = dict()
#A
output["n_review"] = review.count()
#B
review_strip = review.map(lambda s: s.strip("{}"))
review_split = review_strip.map(lambda s: s.split(","))
output["n_review_2018"] = review_split.filter(date).count()
#C
output["n_user"] = review_split.map(user_id).distinct().count()
#D
output["top10_user"] = review_split.map(user_id).map(lambda x: [x, 1]).reduceByKey(add).sortByKey(True).map(swap).sortByKey(False).map(swap).take(10)
#E
output["n_business"] = review_split.map(bussiness_id).distinct().count()
#F
output["top10_business"] = review_split.map(bussiness_id).map(lambda x: [x, 1]).reduceByKey(add).sortByKey(True).map(swap).sortByKey(False).map(swap).take(10)

print(output)
# with open('task1.json', 'w') as outfile:
json.dump(output, output_file)
output_file.close()
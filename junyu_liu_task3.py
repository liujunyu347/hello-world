from pyspark import SparkContext
from pyspark import SparkConf
import codecs
from sys import argv
import json
import time
def id_and_star(line):
    return (line[2][14:-1], line[3][7:])
def id_and_city(line):
    return (line[0][15:-1], line[3][7:-1])
def avg(iterator):
    sum_star, count = 0, 0
    #print(iterator[0])
    for i in iterator[1]:
        #print(i)
        sum_star += float(i)
        count += 1
    return (iterator[0], sum_star/count)
def swap(x):
    return (x[1], x[0])

input_file1 = argv[1]
input_file2 = argv[2]

sc = SparkContext("local[*]")
sf = SparkConf()
sf.set("spark.executor.memory", "4g")
review = sc.textFile(input_file1).coalesce(12)
business = sc.textFile(input_file2).coalesce(12)
output = dict()

review_for_join = review.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['stars']))
business_for_join = business.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['city']))
#print(review_for_join.collect())
#print(business_for_join.collect())
joined_two = review_for_join.join(business_for_join)


print_top = joined_two.map(lambda x: (x[1][1], x[1][0])).groupByKey().map(avg).sortByKey(True).map(swap).sortByKey(False).map(swap)
#Method1
start_time = time.time()
#print(print_top.collect()[0:10])
m1 = print_top.collect()[0:10]
for city in m1:
    print(str(city[0].encode("utf-8"))+str(city[1]))
end_time = time.time()
output["m1"] = end_time - start_time
#Method2
start_time = time.time()
#print(print_top.take(10))
m2 = print_top.take(10)
for city in m2:
    print(str(city[0].encode("utf-8"))+str(city[1]))
end_time = time.time()
output["m2"] = end_time - start_time

output["explaination"] = "collect function will collect all the city and stars into a list and then return the first ten elements, " \
                         "but take only takes ten first elements into a list, which processes less elements resulting less running time."
print(output)
#with open('task3.txt', 'w') as outfile:
with open(argv[3], "w", encoding='utf-8') as output_file1:
    output_file1.write("city,stars\n")
    for city in print_top.collect():
        output_file1.write(str(city[0]) + "," + str(city[1]) + "\n")
output_file1.close()
#with open('task3.json', 'w') as outfile:
with open(argv[4], "w", encoding='utf-8') as output_file2:
    json.dump(output, output_file2)
output_file2.close()
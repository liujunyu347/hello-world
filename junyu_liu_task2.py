from pyspark import SparkContext
from pyspark import SparkConf
import itertools
import time
import json
from sys import argv
start = time.time()
Filter_Threshold = 70
SUPPORT = 50
input_file = "AZ_yelp.csv"
output_file = "junyu_liu_task2.txt"
input_file1 = "review.json"
input_file2 = "business.json"
# Filter_Threshold = int(argv[1])
# SUPPORT = int(argv[2])
# input_file = argv[3]
# output_file = argv[4]

support = int(SUPPORT/2)
# def data_preprocess():
#     review = sc.textFile(input_file1).coalesce(12)
#     business = sc.textFile(input_file2).coalesce(12)
#     review_for_join = review.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['user_id']))
#     business_for_join = business.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['state'])).filter(
#         lambda x: x[1] == 'CA')
#     joined_two = review_for_join.join(business_for_join).map(lambda x: (x[1][0], x[0]))
#     # print(joined_two.count())
#     with open('task2_dataset.csv', "w", encoding='utf-8') as output_file1:
#         output_file1.write("user_id,business_id\n")
#         for line in joined_two.collect():
#             output_file1.write(str(line[0]) + "," + str(line[1]) + "\n")
#     output_file1.close()
def strip_header(line):
    if "user_id,business_id" not in line:
        return line
def swap(x):
    return (x[1], x[0])
def update_frequent_single(sure_frequent):
    updated = set()
    for a_tuple in sure_frequent:
        for elem in a_tuple:
            updated.add(elem)
    return updated
def check_frequent_in_last_layer(a_tuple, last_layer, last_layer_length):
    a_tuple = list(a_tuple)
    subtuples = list(itertools.combinations(a_tuple, last_layer_length))
    for subtuple in subtuples:
        if last_layer_length == 1:
            if subtuple[0] not in last_layer:
                return False
        else:
            if subtuple not in last_layer:
                return False
    # print("return1 True")
    return True
def find_candidates(iterater):
    partition_list = [x[1] for x in iterater]
    count_single = dict()
    largest_busket_len = 0
    frequent_single = set()
    candidates = dict()
    #counts how many times a number in this chunk
    for a_list in partition_list:
        if len(a_list) > largest_busket_len:
            largest_busket_len = len(a_list)
        for num in a_list:
            if num not in count_single:
                count_single[num] = 1
            else:
                count_single[num] += 1
    #find frequent_single
    for key, value in count_single.items():
        if value >= support:
            frequent_single.add(key)
    candidates[1] = frequent_single
    #for each length in this partition iterate through all the basket to make combinations and find the candidates
    current_length_frequent_single_filter = sorted(frequent_single)
    for item_length in range(2, largest_busket_len + 1):
        count_item = dict()
        for basket in partition_list:
            current_basket_frequent_single = set()
            for num in basket:
                if num in current_length_frequent_single_filter:
                    current_basket_frequent_single.add(num)
            #generate the combination for finding cadidates for each basket
            item = list(itertools.combinations(sorted(current_basket_frequent_single), item_length))
            for a_tuple in item:
                #condition 1: check the last layer, if every subtuple are frequent
                if check_frequent_in_last_layer(a_tuple, candidates[item_length-1], item_length-1):# a_tuple in candidates[int(item_length)-1]:
                    if a_tuple in count_item:
                        count_item[a_tuple] += 1
                    else:
                        count_item[a_tuple] = 1
        sure_frequent = set()
        for item, item_support in count_item.items():
            if item_support >= support:
                sure_frequent.add(item)
        if len(sure_frequent) > 0:
            #filter frequent single preparing for next length
            current_length_frequent_single_filter = update_frequent_single(sure_frequent)
            candidates[item_length] = sure_frequent
        else:
            break
    # print("candidates: ",end="")
    # print(candidates)
    output = set()
    if len(candidates[1]) > 0:
        for key in candidates:
            output |= candidates[key]
        yield output
def find_frequent_itemset(iterater, candidates):
    output = []
    for candidate in candidates:
        if type(candidate) is str:
            if candidate in iterater[1]:
                output.append((candidate, 1))
        elif type(candidate) is tuple:
            flag = True
            for num in candidate:
                if num not in iterater[1]:
                    flag = False
                    break
            if flag:
                output.append((candidate, 1))
    return output
def output_txt(output_file, candidates, itemsets):
    with open(output_file, "w", encoding='utf-8') as output:
        candidates_dict = {1: set()}
        itemsets_dict = {1: set()}
        for candidate in candidates:
            if type(candidate) is str:
                candidates_dict[1].add(candidate)
            else:
                if len(candidate) in candidates_dict:
                    candidates_dict[len(candidate)].add(candidate)
                else:
                    candidates_dict[len(candidate)] = set()
                    candidates_dict[len(candidate)].add(candidate)
        for itemset in itemsets:
            if type(itemset) is str:
                itemsets_dict[1].add(itemset)
            else:
                if len(itemset) in itemsets_dict:
                    itemsets_dict[len(itemset)].add(itemset)
                else:
                    itemsets_dict[len(itemset)] = set()
                    itemsets_dict[len(itemset)].add(itemset)
        output.write("Candidates:\n")
        for length in range(1, max(set(candidates_dict.keys()))+1):
            if length == 1:
                for elem in sorted(list(candidates_dict[length])):
                    if elem != sorted(list(candidates_dict[length]))[-1]:
                        output.write("('" + elem + "')" + ",")
                    else:
                        output.write("('" + elem + "')")
            else:
                output.write(str(sorted(list(candidates_dict[length]))).replace("[", "").replace("]", "").replace("), (", "),("))
            output.write("\n\n")
        output.write("Frequent Itemsets:\n")
        for length in range(1, max(set(itemsets_dict.keys()))+1):
            if length == 1:
                for elem in sorted(list(itemsets_dict[length])):
                    if elem != sorted(list(itemsets_dict[length]))[-1]:
                        output.write("('" + elem + "')" + ",")
                    else:
                        output.write("('" + elem + "')")
            else:
                output.write(str(sorted(list(itemsets_dict[length]))).replace("[", "").replace("]", "").replace("), (", "),("))
            output.write("\n\n")
    output.close()
sc = SparkContext("local[*]")
sf = SparkConf()
sf.set("spark.executor.memory", "4g")
# data_preprocess()
user_business = sc.textFile(input_file).repartition(2)
user_business_split = user_business.filter(strip_header).map(lambda s: s.strip()).map(lambda s: s.split(","))
case1_pre = user_business_split.groupByKey().map(lambda x: [x[0], set(x[1])]).filter(lambda x: len(x[1]) > Filter_Threshold)#distinct
case1_candidate = case1_pre.mapPartitions(find_candidates).flatMap(lambda x: x).distinct().collect()
case1_itemset = case1_pre.map(lambda x: find_frequent_itemset(x, case1_candidate)).flatMap(lambda x: x)\
    .reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= SUPPORT).map(lambda x: x[0])
output_txt(output_file, case1_candidate, case1_itemset.collect())
print(case1_itemset.collect())
print(case1_pre.collect())
print(len(case1_itemset.collect()))
end = time.time()
whole_time = end - start
print("Duration: " + str(whole_time))
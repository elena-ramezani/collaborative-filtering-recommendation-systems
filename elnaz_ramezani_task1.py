#import findspark
#findspark.init("/usr/local/spark", edit_rc=True)
import argparse
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from operator import add
import pdb
import time
import argparse
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from operator import add
import pdb
import time
import sys
import csv
sc = SparkContext("local[*]", "Elnaz App")

train_file_name = sys.argv[1]
output_file = sys.argv[2]


rdd_csv = sc.textFile(train_file_name)


dataHeader = rdd_csv.first()
data = rdd_csv.filter(lambda x: x!= dataHeader).map(lambda x: x.split(','))

read_data1 = rdd_csv.filter(lambda x: x!= dataHeader).map(lambda x: x.split(",")).map(lambda x: (x[0],x[1])).groupByKey()
read_data2 = rdd_csv.filter(lambda x: x!= dataHeader).map(lambda x: x.split(",")).map(lambda x: (x[1],x[0])).groupByKey()

all_user = read_data1.map(lambda x: x[0]).distinct().collect()
all_user.sort()
all_business = read_data2.map(lambda x: x[0]).distinct().collect()
all_business.sort()

all_business_dic = dict()
all_user_dic = dict()

i = 0
for item in all_business:
    all_business_dic[item] = i
    i += 1

j = 0
for item in all_user:
    all_user_dic[item] =j
    j += 1

vu = sc.broadcast(all_user_dic)



hashes_value =[[421, 167, 1610612741], [491, 397, 100663319], [659, 257, 3145739], [479, 193, 201326611],
               [167, 167, 402653189], [619, 139, 393241], [929, 137, 402653189], [389, 211, 393241], [443, 431, 805306457],
               [983, 211, 100663319], [109, 211, 805306457], [761, 389, 1572869], [661, 131, 1610612741],
               [241, 373, 25165843], [491, 163, 12582917], [257, 293, 786433], [317, 191, 402653189],
               [127, 389, 12582917], [467, 347, 3145739], [827, 191, 393241], [617, 211, 3145739], [127, 241, 25165843],
               [757, 233, 805306457], [641, 337, 196613], [547, 233, 1610612741], [233, 307, 1610612741],
               [457, 271, 100663319], [937, 173, 805306457], [953, 107, 1572869], [331, 277, 201326611],
               [967, 197, 100663319], [919, 149, 25165843], [607, 151, 402653189], [811, 167, 393241], [331, 419, 6291469],
               [157, 439, 3145739], [821, 131, 402653189], [859, 449, 393241], [809, 103, 393241], [151, 193, 201326611],
               [709, 383, 201326611], [241, 397, 1610612741], [239, 257, 50331653], [769, 433, 49157], [601, 227, 196613],
               [823, 127, 50331653], [701, 223, 786433], [281, 137, 402653189], [719, 233, 12582917], [839, 419, 196613],
               [761, 283, 1572869], [677, 421, 402653189], [727, 359, 786433], [613, 283, 100663319], [487, 157, 196613],
               [619, 281, 25165843], [277, 181, 201326611], [509, 131, 12582917], [167, 251, 98317], [751, 277, 393241],
               [409, 443, 25165843], [509, 331, 6291469], [241, 223, 196613], [491, 233, 402653189],
               [541, 389, 100663319], [563, 199, 402653189], [359, 179, 402653189], [109, 313, 3145739], [719, 113, 393241],
               [709, 173, 196613], [727, 233, 3145739], [557, 317, 786433], [809, 131, 25165843], [983, 383, 1572869],
               [683, 227, 805306457], [557, 109, 98317], [193, 373, 786433], [797, 337, 100663319], [991, 283, 402653189],
               [479, 281, 805306457], [997, 353, 402653189], [503, 431, 201326611], [907, 173, 786433],
               [521, 317, 50331653], [157, 197, 805306457], [761, 149, 402653189], [811, 311, 3145739],
               [577, 449, 201326611], [227, 173, 50331653], [829, 419, 6291469], [191, 277, 49157], [743, 389, 196613],
               [619, 353, 6291469], [821, 337, 201326611], [151, 103, 786433], [389, 383, 6291469], [911, 277, 805306457],
               [751, 127, 402653189], [739, 227, 805306457], [751, 227, 201326611], [173, 401, 1572869],
               [751, 149, 3145739], [223, 311, 201326611], [359, 163, 3145739], [883, 239, 196613], [563, 307, 805306457],
               [379, 367, 196613], [997, 269, 100663319], [103, 181, 196613], [599, 431, 100663319], [107, 151, 50331653],
               [683, 173, 12582917], [809, 167, 49157], [577, 241, 12582917], [937, 101, 98317], [431, 433, 3145739],
               [811, 401, 1572869], [433, 337, 100663319], [823, 433, 786433], [239, 293, 100663319]]

mat = data.map(lambda x: (x[1], [vu.value[x[0]]])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
matrix = mat.collect()

m = len(all_user_dic)

def create_signature(x, has):
    first_value = has[0]
    second_value = has[1]
    third_value = has[2]

    return min([((first_value * index_user + second_value) % third_value) % m for index_user in x[1]])




signatures = mat.map(lambda x: (x[0], [create_signature(x, has) for has in hashes_value]))

number_hash_function = len(hashes_value)
number_band = 40
row = int(number_hash_function / number_band)


def devide_to_bans(x):

    result = []
    for i in range(number_band):
        result.append(((i, tuple(x[1][i * row:(i + 1) * row])), [x[0]]))
    return result


def create_pairs(signature):
    result = []
    length = len(signature[1])
    inside_list = list(signature[1])
    inside_list.sort()
    for i in range(length):
        for j in range(i + 1, length):
            result.append(((inside_list[i], inside_list[j]), 1))
    return result



candidate_pair = signatures.flatMap(devide_to_bans).reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1]) > 1).flatMap(create_pairs) \
    .reduceByKey(lambda x, y: x).map(lambda x: x[0])


def jaccard_similarity(x):
    first_bus = set(matrix[all_business_dic[x[0]]][1])
    second_bus = set(matrix[all_business_dic[x[1]]][1])
    inter = first_bus & second_bus
    union = first_bus | second_bus
    jac_value = len(inter) / len(union)
    return (x[0], x[1], jac_value)


result_jacard_similarity = candidate_pair.map(jaccard_similarity).filter(lambda x: x[2] >= 0.5).sortBy(lambda x: x[1]).sortBy(lambda x: x[0])

#my_result = result_jacard_similarity.map(lambda x:(x[0],x[1]))

my_result = result_jacard_similarity.collect()
'''''
length_my_result = len(my_result.collect())
result_ground = ground_file.filter(lambda x: x!= dataHeader).map(lambda x: x.split(',')).map(lambda x: (x[0],x[1]))
length_ground = len(result_ground.collect())

tp = my_result.intersection(result_ground)

length_union_result = len(tp.collect())
precision = length_union_result/length_my_result
recall = length_union_result/length_ground
'''''

with open(output_file, 'w') as out:
    csv_out = csv.writer(out)
    csv_out.writerow(['business_id_1', 'business_id_2', 'similarity'])
    for row in my_result:
        csv_out.writerow([row[0], row[1], row[2]])

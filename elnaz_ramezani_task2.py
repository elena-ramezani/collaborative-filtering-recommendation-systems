import findspark
findspark.init("/usr/local/spark", edit_rc=True)
import argparse
from pyspark.context import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import csv
import sys
import time
import math

hashes_value = [[421, 167, 1610612741], [491, 397, 100663319], [659, 257, 3145739], [479, 193, 201326611],
                [167, 167, 402653189], [619, 139, 393241], [929, 137, 402653189], [389, 211, 393241],
                [443, 431, 805306457],
                [983, 211, 100663319], [109, 211, 805306457], [761, 389, 1572869], [661, 131, 1610612741],
                [241, 373, 25165843], [491, 163, 12582917], [257, 293, 786433], [317, 191, 402653189],
                [127, 389, 12582917], [467, 347, 3145739], [827, 191, 393241], [617, 211, 3145739],
                [127, 241, 25165843],
                [757, 233, 805306457], [641, 337, 196613], [547, 233, 1610612741], [233, 307, 1610612741],
                [457, 271, 100663319], [937, 173, 805306457], [953, 107, 1572869], [331, 277, 201326611],
                [967, 197, 100663319], [919, 149, 25165843], [607, 151, 402653189], [811, 167, 393241],
                [331, 419, 6291469],
                [157, 439, 3145739], [821, 131, 402653189], [859, 449, 393241], [809, 103, 393241],
                [151, 193, 201326611],
                [709, 383, 201326611], [241, 397, 1610612741], [239, 257, 50331653], [769, 433, 49157],
                [601, 227, 196613],
                [823, 127, 50331653], [701, 223, 786433], [281, 137, 402653189], [719, 233, 12582917],
                [839, 419, 196613],
                [761, 283, 1572869], [677, 421, 402653189], [727, 359, 786433], [613, 283, 100663319],
                [487, 157, 196613],
                [619, 281, 25165843], [277, 181, 201326611], [509, 131, 12582917], [167, 251, 98317],
                [751, 277, 393241],
                [409, 443, 25165843], [509, 331, 6291469], [241, 223, 196613], [491, 233, 402653189],
                [541, 389, 100663319], [563, 199, 402653189], [359, 179, 402653189], [109, 313, 3145739],
                [719, 113, 393241],
                [709, 173, 196613], [727, 233, 3145739], [557, 317, 786433], [809, 131, 25165843], [983, 383, 1572869],
                [683, 227, 805306457], [557, 109, 98317], [193, 373, 786433], [797, 337, 100663319],
                [991, 283, 402653189],
                [479, 281, 805306457], [997, 353, 402653189], [503, 431, 201326611], [907, 173, 786433],
                [521, 317, 50331653], [157, 197, 805306457], [761, 149, 402653189], [811, 311, 3145739],
                [577, 449, 201326611], [227, 173, 50331653], [829, 419, 6291469], [191, 277, 49157], [743, 389, 196613],
                [619, 353, 6291469], [821, 337, 201326611], [151, 103, 786433], [389, 383, 6291469],
                [911, 277, 805306457],
                [751, 127, 402653189], [739, 227, 805306457], [751, 227, 201326611], [173, 401, 1572869],
                [751, 149, 3145739], [223, 311, 201326611], [359, 163, 3145739], [883, 239, 196613],
                [563, 307, 805306457],
                [379, 367, 196613], [997, 269, 100663319], [103, 181, 196613], [599, 431, 100663319],
                [107, 151, 50331653],
                [683, 173, 12582917], [809, 167, 49157], [577, 241, 12582917], [937, 101, 98317], [431, 433, 3145739],
                [811, 401, 1572869], [433, 337, 100663319], [823, 433, 786433], [239, 293, 100663319]]

def predict(user_id, business_id, best_similar_business):
    # average of active user
    try:
        selected_user_id = users_rdd.value[user_id]
    except:
        bb = [x[1] for x in business_rdd1[business_id]]
        return sum(bb) / len(bb)

    dummy = [x[1] for x in selected_user_id]
    dummy_mean = sum(dummy) / len(dummy)
    best_similar_business = list(set(best_similar_business))

    if len(best_similar_business) == 0:
        return dummy_mean

    elif (len(best_similar_business) == 1) and (best_similar_business[0][1] == business_id):
        return dummy_mean

    else:
        num = 0
        den = 0
        for best_buss in best_similar_business:
            bus_id = best_buss[1]

            key = (user_id, bus_id)
            try:
                if key in user_business_rdd.value:
                    # check condition
                    output_rating = user_business_rdd.value[(user_id, bus_id)]
                    den = den + abs(best_buss[0])
                    num = num + best_buss[0] * output_rating
            except:
                num = 0
                den = 0

        if den == 0 or num == 0:
            return dummy_mean
        else:
            pred = num / den
            if pred < 0:
                pred = -1 * pred
            return pred

def predict_similar_users(active_user, active_bus, top_similar):
    # average of active user
    try:
        active_user_data = users_rdd1[active_user]
    except:
        bb = [x[1] for x in business_rdd1[active_bus]]
        return sum(bb) / len(bb)


    a_temp = [x[1] for x in active_user_data]
    a_mean = sum(a_temp) / len(a_temp)
    aa = len(top_similar)

    if aa == 0:
        return a_mean

    else:
        num = 0.0
        den = 0.0
        for item in top_similar:
            other_user = item[1]
            key = (other_user, active_bus)

            try:
                if key in user_business_rdd1:
                    o_rating = user_business_rdd1[key]
                    other_user_data = users_rdd1[other_user]
                    o_temp = [x[1] for x in other_user_data if other_user_data[0] != active_bus]
                    o_mean = sum(o_temp)/len(o_temp)
                    den = den + abs(item[0])
                    num = num + item[0]*(o_rating - o_mean)
            except:
                num = 0.0
                den = 0.0

        if den == 0 or num == 0:
            return a_mean
        else:
            return (a_mean + num/den)



def pearsonCorrelation(selected_business, all_business):
    corrated = list()
    i = 0
    j = 0
    selected_business.sort()
    all_business.sort()
    while (i < len(selected_business) and j < len(all_business)):
        if selected_business[i][0] == all_business[j][0]:
            corrated.append((selected_business[i][0], (selected_business[i][1], all_business[j][1])))
            i = i + 1
            j = j + 1
        elif selected_business[i][0] < all_business[j][0]:
            i = i + 1
        else:
            j = j + 1

    if len(corrated) == 0 or len(corrated) == 1:
        return -2.0

    active = [x[1][0] for x in corrated]
    dummy_mean = sum(active) / len(active)

    other = [x[1][1] for x in corrated]
    o_mean = sum(active) / len(active)

    a_list = active
    o_list = other

    num = 0.0
    d1 = 0.0
    d2 = 0.0
    for i in range(len(a_list)):
        a = a_list[i] - dummy_mean
        o = o_list[i] - o_mean
        num = num + a * o
        d1 = d1 + (a * a)
        d2 = d2 + (o * o)

    den = math.sqrt(d1) * math.sqrt(d2)
    if den == 0 or num == 0:
        return -2.0
    else:
        return num / den


def find_similar(user_id, business_id):
    best_similar_business = list()
    if business_id not in business_rdd.value:
        # not rated yet
        best_similar_business.append((1, business_id))
        return best_similar_business

    selected_business = business_rdd.value[business_id]

    selected_user_id = users_rdd.value[user_id]
    all_businesses_id = [x[0] for x in selected_user_id]
    for bus_id in all_businesses_id:
        if business_id != bus_id:
            all_business = business_rdd.value[bus_id]
            similarity = pearsonCorrelation(selected_business, all_business)
            if similarity != -2.0:
                best_similar_business.append((similarity, bus_id))
            else:
                best_similar_business.append((1, business_id))

    similar_business = sorted(best_similar_business, reverse=True)
    return similar_business[:5]  # Neighbourhood value 5


def similar_user(acive_user, active_bus):
    similar_users = list()

    if active_bus not in business_rdd2:
        # item cold start
        similar_users.append((0, acive_user))
        return similar_user

    active_user_data = users_rdd1[acive_user]
    other_users = business_rdd2[active_bus]

    for other_user in other_users:
        if acive_user != other_user:

            other_user_data = users_rdd1[other_user]
            similarity = pearsonCorrelation(active_user_data, other_user_data)
            if similarity != -2.0:
                similar_users.append((similarity, other_user))

    similar_users = sorted(similar_users, reverse=True)
    return  similar_users


def append(a, b):
    a.append(b)
    return a


def create_signature(x, has):
    first_value = has[0]
    second_value = has[1]
    third_value = has[2]

    return min([((first_value * index_user + second_value) % third_value) % m for index_user in x[1]])


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

def jaccard_similarity(x):
    first_bus = set(matrix[all_business_dic[x[0]]][1])
    second_bus = set(matrix[all_business_dic[x[1]]][1])
    inter = first_bus & second_bus
    union = first_bus | second_bus
    jac_value = len(inter) / len(union)
    return (x[0], x[1], jac_value)

def similarity_with_lsh(active_bus):
    top_similar_bus = list()
    if (active_bus not in dic_similar_business1) and (active_bus not in dic_similar_business2):
        # no movie similar to active movie
        # assign average rating of user
        top_similar_bus.append((1, active_bus))
        return top_similar_bus

    if active_bus not in business_rated_by_user:
        # item never rated by any user (item cold start)
        top_similar_bus.append((1, active_bus))
        return top_similar_bus

    active_bus_data = business_rated_by_user[active_bus]
    other_bus = list()
    if active_bus in dic_similar_business1:
        other_bus = other_bus + dic_similar_business1[active_bus]
    if active_bus in dic_similar_business2:
        other_bus = other_bus + dic_similar_business2[active_bus]

    other_bus = list(set(other_bus))

    for other in other_bus:
        if active_bus != other:
            other_bus_data = business_rated_by_user[other]
            similarity = pearsonCorrelation(active_bus_data, other_bus_data)
            if similarity != -2:
                top_similar_bus.append((similarity, other_bus))
            else:
                top_similar_bus.append((1, active_bus))

    #similarMovies = sorted(top_similar_bus, reverse=True)
    #return similarMovies
    return top_similar_bus



t1 = time.time()
training_file = sys.argv[1]  # 'data/yelp_train.csv'
validation_file = sys.argv[2]  # 'data/yelp_val.csv'
case_number = sys.argv[3]
output_file = sys.argv[4]

sc = SparkContext("local[*]", "CF")


if int(case_number) == 3:  ## item based CF
    training_rdd = sc.textFile(training_file, minPartitions=None, use_unicode=True)
    training_rdd = training_rdd.mapPartitions(lambda x: csv.reader(x))
    training_header = training_rdd.first()

    training_rdd = training_rdd.filter(lambda x: x != training_header)
    training_rdd = training_rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    validation_rdd = sc.textFile(validation_file, minPartitions=None, use_unicode=True)
    validation_rdd = validation_rdd.mapPartitions(lambda x: csv.reader(x))
    testing_header = validation_rdd.first()
    validation_rdd = validation_rdd.filter(lambda x: x != testing_header)
    test_rdd_with_res = validation_rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    training_rdd_cleaned = training_rdd.subtractByKey(validation_rdd)
    training_rdd_cleaned = training_rdd_cleaned.map(lambda x: (x[0][0], (x[0][1], x[1])))
    test_rdd = test_rdd_with_res.map(lambda x: (x[0][0], x[0][1])).sortByKey()

    users_rdd1 = training_rdd_cleaned.groupByKey().sortByKey().mapValues(list).collectAsMap()
    user_business_rdd1 = training_rdd_cleaned.map(lambda x: ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()
    business_rdd1 = training_rdd_cleaned.map(lambda x: (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(
        list).collectAsMap()
    business_rdd2 = training_rdd_cleaned.map(lambda x: (x[1][0], x[0])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()

    users_rdd = sc.broadcast(users_rdd1)
    user_business_rdd = sc.broadcast(user_business_rdd1)
    business_rdd = sc.broadcast(business_rdd1)

    all_user_train = training_rdd.map(lambda x: x[0][0]).distinct().collect()
    all_user_val = test_rdd_with_res.map(lambda x: x[0][0]).distinct().collect()

    all_business_train = training_rdd.map(lambda x: x[0][1]).distinct().collect()
    all_business_val = test_rdd_with_res.map(lambda x: x[0][1]).distinct().collect()

    all_business_dic = dict()
    all_user_dic = dict()

    i = 0
    for item in all_business_train:
        if item not in all_business_dic:
            all_business_dic[item] = i
            i += 1
    for item in all_business_val:
        if item not in all_business_dic:
            all_business_dic[item] = i
            i += 1

    j = 0
    for item in all_user_train:
        if item not in all_user_dic:
            all_user_dic[item] = j
            j += 1
    for item in all_user_val:
        if item not in all_user_dic:
            all_user_dic[item] = j
            j += 1

    vu = sc.broadcast(all_user_dic)

    rdd_csv = sc.textFile(training_file)

    dataHeader = rdd_csv.first()
    data = rdd_csv.filter(lambda x: x != dataHeader).map(lambda x: x.split(','))

    mat = data.map(lambda x: (x[1], [vu.value[x[0]]])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
    matrix = mat.collect()

    m = len(all_user_dic)

    signatures = mat.map(lambda x: (x[0], [create_signature(x, has) for has in hashes_value]))

    number_hash_function = len(hashes_value)
    number_band = 40
    row = int(number_hash_function / number_band)

    candidate_pair = signatures.flatMap(devide_to_bans).reduceByKey(lambda x, y: x + y).filter(
        lambda x: len(x[1]) > 1).flatMap(create_pairs) \
        .reduceByKey(lambda x, y: x).map(lambda x: x[0])

    result_jacard_similarity = candidate_pair.map(jaccard_similarity).sortBy(
        lambda x: x[1]).sortBy(lambda x: x[0])

    all_data = training_rdd
    test_key = test_rdd_with_res
    test_data = test_rdd
    train_data = training_rdd_cleaned
    usersRdd = users_rdd1
    user_rated_business = user_business_rdd1
    business_rated_by_user = business_rdd1

    data = train_data.map(lambda x: (x[0], x[1][0]))  # all user and bussiness

    similar_bussiness = candidate_pair.map(jaccard_similarity).filter(lambda x: x[2] >= 0.5).map(
        lambda x: ((min(x[0], x[1]), max(x[0], x[1])), x[2]))

    dic_similar_business1 = similar_bussiness.map(lambda x: (x[0][0], x[0][1])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()
    dic_similar_business2 = similar_bussiness.map(lambda x: (x[0][1], x[0][0])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()

    best_similar_businesses = test_rdd.map(lambda x: (x[0], x[1], similarity_with_lsh(x[1])))
    prediction = best_similar_businesses.map(lambda x: (x[0], x[1], predict(x[0], x[1], x[2])))
    results = prediction.map(lambda x: ((x[0], x[1]), x[2])).join(test_rdd_with_res)
    differences = results.map(lambda x: abs(x[1][0] - x[1][1]))
    RMSE = math.sqrt(differences.map(lambda x: x ** 2).mean())

    print('RMSE: ', RMSE)

    res = prediction.collect()

    with open(output_file, 'w+') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['user_id', 'business_id', 'prediction'])
        for row in res:
            csv_out.writerow([row[0], row[1], round(row[2], 2)])
    out.close()
    print('time: ', time.time() - t1)


if int(case_number) == 1:  ## model based CF
    training_rdd = sc.textFile(training_file, minPartitions=None, use_unicode=True)
    training_rdd = training_rdd.mapPartitions(lambda x: csv.reader(x))
    training_header = training_rdd.first()

    training_rdd = training_rdd.filter(lambda x: x != training_header)
    training_rdd = training_rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    validation_rdd = sc.textFile(validation_file, minPartitions=None, use_unicode=True)
    validation_rdd = validation_rdd.mapPartitions(lambda x: csv.reader(x))
    testing_header = validation_rdd.first()
    validation_rdd = validation_rdd.filter(lambda x: x != testing_header)
    test_rdd_with_res = validation_rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    training_rdd_cleaned = training_rdd.subtractByKey(validation_rdd)
    training_rdd_cleaned = training_rdd_cleaned.map(lambda x: (x[0][0], (x[0][1], x[1])))
    test_rdd = test_rdd_with_res.map(lambda x: (x[0][0], x[0][1])).sortByKey()

    users_rdd1 = training_rdd_cleaned.groupByKey().sortByKey().mapValues(list).collectAsMap()
    user_business_rdd1 = training_rdd_cleaned.map(lambda x: ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()
    business_rdd1 = training_rdd_cleaned.map(lambda x: (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(
        list).collectAsMap()
    business_rdd2 = training_rdd_cleaned.map(lambda x: (x[1][0], x[0])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()

    users_rdd = sc.broadcast(users_rdd1)
    user_business_rdd = sc.broadcast(user_business_rdd1)
    business_rdd = sc.broadcast(business_rdd1)

    all_user_train = training_rdd.map(lambda x: x[0][0]).distinct().collect()
    all_user_val = test_rdd_with_res.map(lambda x: x[0][0]).distinct().collect()

    all_business_train = training_rdd.map(lambda x: x[0][1]).distinct().collect()
    all_business_val = test_rdd_with_res.map(lambda x: x[0][1]).distinct().collect()

    all_business_dic = dict()
    all_user_dic = dict()

    i = 0
    for item in all_business_train:
        if item not in all_business_dic:
            all_business_dic[item] = i
            i += 1
    for item in all_business_val:
        if item not in all_business_dic:
            all_business_dic[item] = i
            i += 1

    j = 0
    for item in all_user_train:
        if item not in all_user_dic:
            all_user_dic[item] = j
            j += 1
    for item in all_user_val:
        if item not in all_user_dic:
            all_user_dic[item] = j
            j += 1

    rev_all_user_dic = {value: key for key, value in all_user_dic.items()}
    rev_all_business_dic = {value: key for key, value in all_business_dic.items()}

    ratings = training_rdd.map(lambda l: Rating(all_user_dic[l[0][0]], all_business_dic[l[0][1]], l[1]))

    rank = 2
    numIterations = 27
    model = ALS.train(ratings, rank, numIterations)
    # model = ALS.trainImplicit(ratings, rank, numIterations, alpha=0.0001)

    test_rdd_int = test_rdd_with_res.map(lambda l: (all_user_dic[l[0][0]], all_business_dic[l[0][1]]))
    predictions = model.predictAll(test_rdd_int).map(lambda r: ((r[0], r[1]), r[2]))

    ratesAndPreds = test_rdd_with_res.map(
        lambda l: ((all_user_dic[l[0][0]], all_business_dic[l[0][1]]), float(l[1]))).join(predictions)

    RMSE = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
    print('RMSE: ', RMSE)

    res = predictions.map(
        lambda x: (rev_all_user_dic[x[0][0]], rev_all_business_dic[x[0][1]], round(x[1], 2))).collect()

    with open(output_file, 'w+') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['user_id', 'business_id', 'prediction'])
        for row in res:
            csv_out.writerow([row[0], row[1], round(row[2], 2)])
    out.close()

    print('time: ', time.time() - t1)

if int(case_number) == 2:  ## user base CF
    training_rdd = sc.textFile(training_file, minPartitions=None, use_unicode=True)
    training_rdd = training_rdd.mapPartitions(lambda x: csv.reader(x))
    training_header = training_rdd.first()

    ## change user with business
    training_rdd = training_rdd.filter(lambda x: x != training_header).map(lambda x: (x[1], x[0], x[2]))
    training_rdd = training_rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    validation_rdd = sc.textFile(validation_file, minPartitions=None, use_unicode=True)
    validation_rdd = validation_rdd.mapPartitions(lambda x: csv.reader(x))
    testing_header = validation_rdd.first()
    ## change user with business
    validation_rdd = validation_rdd.filter(lambda x: x != testing_header).map(lambda x: (x[1], x[0], x[2]))
    test_rdd_with_res = validation_rdd.map(lambda x: ((x[0], x[1]), float(x[2])))

    training_rdd_cleaned = training_rdd.subtractByKey(validation_rdd)
    training_rdd_cleaned = training_rdd_cleaned.map(lambda x: (x[0][0], (x[0][1], x[1])))
    test_rdd = test_rdd_with_res.map(lambda x: (x[0][0], x[0][1])).sortByKey()

    users_rdd1 = training_rdd_cleaned.groupByKey().sortByKey().mapValues(list).collectAsMap()
    user_business_rdd1 = training_rdd_cleaned.map(lambda x: ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()
    business_rdd1 = training_rdd_cleaned.map(lambda x: (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(
        list).collectAsMap()
    business_rdd2 = training_rdd_cleaned.map(lambda x: (x[1][0], x[0])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()

    users_rdd = sc.broadcast(users_rdd1)
    user_business_rdd = sc.broadcast(user_business_rdd1)
    business_rdd = sc.broadcast(business_rdd1)

    all_user_train = training_rdd.map(lambda x: x[0][0]).distinct().collect()
    all_user_val = test_rdd_with_res.map(lambda x: x[0][0]).distinct().collect()

    all_business_train = training_rdd.map(lambda x: x[0][1]).distinct().collect()
    all_business_val = test_rdd_with_res.map(lambda x: x[0][1]).distinct().collect()

    all_business_dic = dict()
    all_user_dic = dict()

    i = 0
    for item in all_business_train:
        if item not in all_business_dic:
            all_business_dic[item] = i
            i += 1
    for item in all_business_val:
        if item not in all_business_dic:
            all_business_dic[item] = i
            i += 1

    j = 0
    for item in all_user_train:
        if item not in all_user_dic:
            all_user_dic[item] = j
            j += 1
    for item in all_user_val:
        if item not in all_user_dic:
            all_user_dic[item] = j
            j += 1

    vu = sc.broadcast(all_user_dic)

    rdd_csv = sc.textFile(training_file)

    dataHeader = rdd_csv.first()
    data = rdd_csv.filter(lambda x: x != dataHeader).map(lambda x: x.split(',')).map(lambda x: (x[1], x[0], x[2]))

    mat = data.map(lambda x: (x[1], [vu.value[x[0]]])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
    matrix = mat.collect()

    m = len(all_user_dic)

    signatures = mat.map(lambda x: (x[0], [create_signature(x, has) for has in hashes_value]))

    number_hash_function = len(hashes_value)
    number_band = 40
    row = int(number_hash_function / number_band)

    candidate_pair = signatures.flatMap(devide_to_bans).reduceByKey(lambda x, y: x + y).filter(
        lambda x: len(x[1]) > 1).flatMap(create_pairs) \
        .reduceByKey(lambda x, y: x).map(lambda x: x[0])

    result_jacard_similarity = candidate_pair.map(jaccard_similarity).sortBy(
        lambda x: x[1]).sortBy(lambda x: x[0])

    all_data = training_rdd
    test_key = test_rdd_with_res
    test_data = test_rdd
    train_data = training_rdd_cleaned
    usersRdd = users_rdd1
    user_rated_business = user_business_rdd1
    business_rated_by_user = business_rdd1

    data = train_data.map(lambda x: (x[0], x[1][0]))  # all user and bussiness

    similar_bussiness = candidate_pair.map(jaccard_similarity).filter(lambda x: x[2] > 0).map(
        lambda x: ((min(x[0], x[1]), max(x[0], x[1])), x[2]))

    dic_similar_business1 = similar_bussiness.map(lambda x: (x[0][0], x[0][1])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()
    dic_similar_business2 = similar_bussiness.map(lambda x: (x[0][1], x[0][0])).groupByKey().sortByKey().mapValues(
        list).collectAsMap()

    best_similar_businesses = test_rdd.map(lambda x: (x[0], x[1], similarity_with_lsh(x[1])))
    prediction = best_similar_businesses.map(lambda x: (x[0], x[1], predict_similar_users(x[0], x[1], x[2])))
    results = prediction.map(lambda x: ((x[0], x[1]), x[2])).join(test_rdd_with_res)
    differences = results.map(lambda x: abs(x[1][0] - x[1][1]))
    RMSE = math.sqrt(differences.map(lambda x: x ** 2).mean())

    print('RMSE: ', RMSE)

    res = prediction.collect()

    with open(output_file, 'w+') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['user_id', 'business_id', 'prediction'])
        for row in res:
            csv_out.writerow([row[1], row[0], round(row[2], 2)])
    out.close()
    print('time: ', time.time() - t1)

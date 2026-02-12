import numpy as np
import math
import os
import sys
import time

from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext, SparkConf


# MAKE A TUPLE OBTAINED FROM SEPARATING COORDINATES (CONVERTED TO FLOATS) AND GROUP
def parse_coors_group(point):
    conv_list = point.split(',')  # TYPE -> LIST
    temp_list = []
    for i in range(0, len(conv_list) - 1):
        temp_list.append(float(conv_list[i]))
    conv_point_coors = tuple(temp_list)  # TYPE -> TUPLE
    conv_point_group = conv_list[-1]  # TYPE -> STR
    conv_point = (conv_point_coors, conv_point_group)  # output point is a tuple: ((coordinates), group)
    return conv_point

# MAKE A TUPLE OBTAINED FROM SEPARATING GROUP AND COORDINATES (CONVERTED TO FLOATS)
def parse_coors_group_inverse(point):
    conv_list = point.split(',')  # TYPE -> LIST
    temp_list = []
    for i in range(0, len(conv_list) - 1):
        temp_list.append(float(conv_list[i]))
    conv_point_coors = tuple(temp_list)  # TYPE -> TUPLE
    conv_point_group = conv_list[-1]  # TYPE -> STR
    conv_point = (conv_point_group, conv_point_coors)  # output point is a tuple: ((coordinates), group)
    return conv_point

# COMPUTE THE EUCLIDEAN DISTANCE BETWEEN TWO POINTS
def euclidean_distance(point, centroid):
    sum = 0
    for i in range(0, len(centroid)):
        sum += ((point[i] - centroid[i]) ** 2)
    return math.sqrt(sum)

# COMPUTE THE MIN SQUARED DISTANCE BETWEEN TWO POINTS
def min_squared_distance_inverse(point, centroids):
    group = point[0]
    distance = min(euclidean_distance(point[1], c) for c in centroids)
    return group, (distance ** 2)

# RETRIEVE THE CENTROID OF THE CLUSTER A POINT BELONGS TO
def calc_center(point, centroids):
    closest_centroid = 0
    min_dist = math.inf
    for i in range(0, len(centroids)):
        dist = euclidean_distance(point[0], centroids[i]) # compute euclidean distance
        if dist < min_dist:
            min_dist = dist
            closest_centroid = i
    centr_point_group = (closest_centroid, point)  # output is a tuple: (centroid index, point)
    return centr_point_group


def MRComputeFairObjective(U, C):
    """
    MapReduce
    - MAP PHASE (R1): .map() takes each initial string and maps it into key, value pairs where the key is the group and the value is the coordinates of point.
                      The keys are not A o B, but we use 0 and 1 respectively. It can be useful at the end (comparation).
    - REDUCE PHASE (R1): .flatMap() compute the sum of the minimun squared distance between a single point and each centroid. The output will be a rdd that contains key, value where key is the group and value
                         is the minimum distance of each point.
    - REDUCE PHASE (R2): .groupByKey() can be considered like an union of two rounds.
                         In the first one we consider aggregation in parallel of two groups (0 or 1) and each one contains a list of values computed by min_squared_distance_inverse() method.
                         In the second we aggregate all the values by the function sum() and we obtain the unique value for each group.

    """

    distance = (U.map(parse_coors_group_inverse)  # <-- MAP PHASE (R1)
                .flatMap(lambda x: [min_squared_distance_inverse(x, C)])  # <-- REDUCE PHASE (R1)
                .reduceByKey(lambda x, y: x + y)  # <-- REDUCE PHASE (R2)
                .cache()
                )

    valA = 0
    valB = 0

    for point in distance.collect():
        group, dist = point[0], point[1]
        if group == "A":
            valA = dist
        if group == "B":
            valB = dist

    return max(valA / NA, valB / NB)

# SUM THE COORDINATES OF TWO POINTS
def addTuples(point1, point2):
    point = []
    for i in range(0, len(point1)):
        result = point1[i] + point2[i]
        point.append(result)
    point = tuple(point) # conversion to a tuple
    return point


def computeVectorX(fixed_a, fixed_b, alpha, beta, ell, k):
    gamma = 0.5
    x_dist = [0.0] * k
    power = 0.5
    t_max = 10

    for _ in range(t_max):
        f_a = fixed_a
        f_b = fixed_b
        power /= 2

        for i in range(k):
            temp = (1 - gamma) * beta[i] * ell[i] / (gamma * alpha[i] + (1 - gamma) * beta[i])
            x_dist[i] = temp
            f_a += alpha[i] * temp * temp
            temp = ell[i] - temp
            f_b += beta[i] * temp * temp

        if f_a == f_b:
            break

        gamma = gamma + power if f_a > f_b else gamma - power

    return x_dist


# COMPUTE THE CENTERS RESPECT TO THE RESULTS OBTAINED BY computeVectorX
def computeCentersC(vec_x, MA_list, MB_list, l_list, K):
    centr = []
    for i in range(0, K):
        first = []
        second = []

        if l_list[i] == 0.0:
            ci_new = MA_list[i]
            centr.append(ci_new)
            continue

        for index in range(0, len(MA_list[i])):
            num_first = (l_list[i] - vec_x[i]) * MA_list[i][index]
            first.append(num_first)
            num_second = vec_x[i] * MB_list[i][index]
            second.append(num_second)
        num1 = tuple(first) # coefficient for points with label A
        num2 = tuple(second) # coefficient for points with label B
        sum_tuple = addTuples(num1, num2)
        div_tuple = []
        for index in range(0, len(sum_tuple)):
            if l_list[i] == 0:
                div_tuple.append(0)
            else:
                num = sum_tuple[index]
                val = num / l_list[i]
                div_tuple.append(val)
        ci_new = tuple(div_tuple)
        centr.append(ci_new)
    return centr

# FOR EACH CLUSTER, COMPUTE THE VALUE OF ALPHA, BETA, MEANA, MEANB, LI, DELTA_A AND DELTA_B, EVEN WITH THE CASE WITHOUT POINTS OF A OR B
def compute_stat_for_pair(pair):

    cluster = pair[0]
    list_points = list(pair[1])
    pointsA = []
    pointsB = []

    for p in list_points:
        if p[1] == 'A':
            pointsA.append(p[0])
        else:
            pointsB.append(p[0])

    dimA = len(pointsA)
    dimB = len(pointsB)
    alpha = dimA / NA
    beta = dimB / NB
    meanA = tuple(0 for _ in range(dimA))
    meanB = tuple(0 for _ in range(dimB))
    li = 0
    delta_A = 0
    delta_B = 0

    if dimB == 0:
        meanA = np.mean(pointsA, axis=0)
        meanA = tuple(float(meanA[i]) for i in range(len(meanA))) # conversion from numpy array to a tuple of float
        meanB = meanA
        delta_A = sum(euclidean_distance(p, meanA) ** 2 for p in pointsA)
        delta_B = 0.0

    elif dimA == 0:
        meanB = np.mean(pointsB, axis=0)
        meanB = tuple(float(meanB[i]) for i in range(len(meanB))) # conversion from numpy array to a tuple of float
        meanA = meanB
        delta_B = sum(euclidean_distance(p, meanB) ** 2 for p in pointsB)
        delta_A = 0.0

    elif dimA > 0 and dimB > 0:
        meanA = np.mean(pointsA, axis=0)
        meanA = tuple(float(meanA[i]) for i in range(len(meanA))) # conversion from numpy array to a tuple of float
        meanB = np.mean(pointsB, axis=0)
        meanB = tuple(float(meanB[i]) for i in range(len(meanB))) # conversion from numpy array to a tuple of float
        li = euclidean_distance(meanA, meanB)
        delta_A = sum(euclidean_distance(p, meanA) ** 2 for p in pointsA)
        delta_B = sum(euclidean_distance(p, meanB) ** 2 for p in pointsB)

    value = [[alpha], [beta], [meanA], [meanB], [li], delta_A, delta_B]

    result = (0, tuple(value)) # returns (0, (alpha, beta, meanA, meanB, li, delta_A, delta_B))
    return result


def union_stat_pairs(pair1, pair2):

    #for aech list of values, we extend the actual list with another list
    alpha_list = []
    alpha_list.extend(pair1[0])
    alpha_list.extend(pair2[0])
    beta_list = []
    beta_list.extend(pair1[1])
    beta_list.extend(pair2[1])
    meanA_list = []
    meanA_list.extend(pair1[2])
    meanA_list.extend(pair2[2])
    meanB_list = []
    meanB_list.extend(pair1[3])
    meanB_list.extend(pair2[3])
    li_list = []
    li_list.extend(pair1[4])
    li_list.extend(pair2[4])
    # delta_A e delta_B are not list, but float
    delta_A = pair1[5] + pair2[5]
    delta_B = pair1[6] + pair2[6]

    value = [alpha_list, beta_list, meanA_list, meanB_list, li_list, delta_A, delta_B]
    result = tuple(value)
    return result


def compute_stats(U, C):
    clusters = (U.flatMap(lambda point: [calc_center(parse_coors_group(point), C)]) # <-- MAP PHASE (R1)
                .groupByKey() # <-- GROUPING + SHUFFLE
                .flatMap(lambda x: [compute_stat_for_pair(x)]) # <-- REDUCE PHASE (R1)
                .reduceByKey(lambda x, y: union_stat_pairs(x, y)).cache() # REDUCE PHASE (R2)
                )

    stats = clusters.collect()[0][1]
    return stats


def MRFairLoyd(U, K, M):
    inputPoints = U.map(lambda x: Vectors.dense(parse_coors_group(x)[0])).cache()
    kmeans = KMeans.train(inputPoints, k=K, maxIterations=0, initializationMode="k-means||")
    C = kmeans.clusterCenters

    for m in range(0, M):
        # store the values in variables or lists
        stats = compute_stats(U, C)
        alpha_list = stats[0]
        beta_list = stats[1]
        MA_list = stats[2]
        MB_list = stats[3]
        l_list = stats[4]
        delta_A = stats[5]
        delta_B = stats[6]

        fixed_A = delta_A / NA
        fixed_B = delta_B / NB

        vec_x = computeVectorX(fixed_A, fixed_B, alpha_list, beta_list, l_list, K)
        C = computeCentersC(vec_x, MA_list, MB_list, l_list, K)

    return C


def main():
    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 5, "Write the command line G16HW1.py <file_name> <L> <K> <M>"

    # SPARK SETUP
    conf = SparkConf().setAppName('G16HW2')
    sc = SparkContext(conf=conf)

    # DATA FROM COMMAND LINE
    file_path = sys.argv[1]

    L = sys.argv[2]
    assert L.isdigit(), "L must be an integer"
    L = int(L)

    K = sys.argv[3]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    M = sys.argv[4]
    assert M.isdigit(), "M must be an integer"
    M = int(M)

    # PRINTS THE COMMAND-LINE ARGUMENTS AND STORES L, K, M INTO SUITABLE VARIABLES
    print(f"Input file = {file_path}, L = {L}, K = {K}, M = {M}")

    # READ THE INPUT POINTS INTO AN RDD OF (point, group) PAIRS, SUBDIVIDED INTO L PARTITIONS
    inputPoints = sc.textFile(file_path).repartition(numPartitions=L).cache()

    # PRINTS THE NUMBER N OF POINTS, THE NUMBER NA OF POINTS OF A GROUP A, AND NUMBER NB OF POINTS OF GROUP B
    A = inputPoints.filter(lambda x: x[-1] == 'A').cache()
    B = inputPoints.filter(lambda x: x[-1] == 'B').cache()

    global NA, NB
    NA = A.count()
    NB = B.count()

    print(f"N = {inputPoints.count()}, NA = {NA}, NB = {NB}")

    # COMPUTE C stand OF K CENTROIDS FOR THE INPUT POINTS WITH M ITERATIONS
    points = inputPoints.map(lambda x: Vectors.dense(parse_coors_group(x)[0])).cache()
    start_time_stand = time.time()
    kmeans = KMeans.train(points, k=K, maxIterations=M, initializationMode="k-means||")
    end_time_stand = time.time()
    Cstand = kmeans.clusterCenters

    # COMPUTE C fair OF K CENTROIDS BY RUNNING MRFairLoyd(inputPoints, K, M)
    start_time_fair = time.time()
    Cfair = MRFairLoyd(inputPoints, K, M)
    end_time_fair = time.time()

    # COMPUTE THE OBJECTIVE FUNCTION FOR C stand AND C fair
    start_time_objstand = time.time()
    objectStand = MRComputeFairObjective(inputPoints, Cstand)
    end_time_objstand = time.time()
    start_time_objfair = time.time()
    objectFair = MRComputeFairObjective(inputPoints, Cfair)
    end_time_objfair = time.time()

    print(f"Fair Objective with Standard Centers = {objectStand}")
    print(f"Fair Objective with Fair Centers = {objectFair}")

    # PRINTS TIME IN SECOND RESPECTIVELY FOR C stand, C fair, OBJECTIVE FUNCTION FOR C stand, OBJECTIVE FUNCTION FOR C fair
    print(f"Time to compute standard centers = {int((end_time_stand - start_time_stand)*1000)} ms")
    print(f"Time to compute fair centers = {int((end_time_fair - start_time_fair)*1000)} ms")
    print(f"Time to compute objective with standard centers = {int((end_time_objstand - start_time_objstand)*1000)} ms")
    print(f"Time to compute objective with fair centers = {int((end_time_objfair - start_time_objfair)*1000)} ms")


if __name__ == "__main__":
    main()
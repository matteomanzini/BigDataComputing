import numpy as np
import math
import os
import sys

from IPython.terminal.shortcuts.auto_match import brackets
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext, SparkConf


# MAKE A TUPLE OBTAINED FROM SEPARATING COORDINATES (CONVERTED TO FLOATS) AND GROUP
def parse_coors_group(point):

    conv_list = point.split(',') # TYPE -> LIST
    temp_list = []
    for i in range(0, len(conv_list)-1):
        temp_list.append(float(conv_list[i]))
    conv_point_coors = tuple(temp_list) # TYPE -> TUPLE
    conv_point_group = conv_list[-1] # TYPE -> STR
    conv_point = (conv_point_coors, conv_point_group)    # output point is a tuple: ((coordinates), group)
    return conv_point


def parse_coors_group_inverse(point):

    conv_list = point.split(',')  # TYPE -> LIST
    temp_list = []
    for i in range(0, len(conv_list) - 1):
        temp_list.append(float(conv_list[i]))
    conv_point_coors = tuple(temp_list)  # TYPE -> TUPLE
    if conv_list[-1] == 'A':
        conv_point_group = 0
    else:
        conv_point_group = 1
    conv_point = (conv_point_group, conv_point_coors)  # output point is a tuple: ((coordinates), group)
    return conv_point


def euclidean_distance(point, centroid):

    sum = 0
    for i in range(0, len(centroid)):
        sum += ((point[i] - centroid[i]) ** 2)
    return math.sqrt(sum)


def min_squared_distance(point, centroids):

    distance = min(euclidean_distance(point[0], c) for c in centroids)
    return  [(distance ** 2)]


def min_squared_distance_inverse(point, centroids):

    group = point[0]
    distance = min(euclidean_distance(point[1], c) for c in centroids)
    return group, (distance ** 2)


# RETRIEVE THE CENTROID OF THE CLUSTER A POINT BELONGS TO
def calc_center(point, centroids):

    closest_centroid = 0
    min_dist = math.inf
    for i in range(0, len(centroids)):
        dist = euclidean_distance(point[0], centroids[i])
        if dist < min_dist:
            min_dist = dist
            closest_centroid = i
    centr_point_group = (closest_centroid, point)    #output is a tuple: (centroid index, point)
    return centr_point_group


def count_points(pairs):
    pairs_dict = {}
    for centroid, point in pairs:
        if centroid not in pairs_dict:
            pairs_dict[centroid] = [0, 0]    # for each new centroid found initialize a new element of the dictionary: "centroid index":[0,0] where centroid index is the key
        if point[1] == 'A':
            pairs_dict[centroid][0] += 1     # "centroid index" : [++1,0] <-- an A labeled point has been found for that centroid
        elif point[1] == 'B':
            pairs_dict[centroid][1] += 1     # "centroid index" : [0,++1] <-- a B labeled point has been found for that centroid

    return [(centr, tuple(pairs_dict[centr])) for centr in pairs_dict.keys()]  # output is a list of tuples where a tuple is: (centroid index,(NA, NB))


def gather_partial_sums(pairs):

    sums = pairs[1]       # pairs[1] contains all partial sums
    triplet = (pairs[0], (sum(s[0] for s in sums), sum(s[1] for s in sums)))   # triplet of statistics: (centroid index, (NA, NB))
    return [triplet]


def MRComputeStandardObjective(U, C):

    """
    MapReduce
    - MAP PHASE: .map() takes each initial string and maps it into key, value pairs where the key is the coordinates and the value if the group A or B.
    - REDUCE PHASE: .flatMap() compute the sum of the minimun squared distance between a single point and each centroid. The output will be a rdd that contains each minimum distance.
    - distance.reduce(): compute the sum of all distances and devided by le number point distinct point.

    """

    distance = (U.map(parse_coors_group) # <-- MAP PHASE
                .groupByKey() # <-- SHUFFLE + GROUPING
                .flatMap(lambda x: min_squared_distance(x, C)) # <-- REDUCE PHASE ROUND 1
    )

    return distance.reduce(lambda x, y: x + y)/U.count()


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

    distance = (U.map(parse_coors_group_inverse) # <-- MAP PHASE (R1)
                .flatMap(lambda x: [min_squared_distance_inverse(x, C)]) # <-- REDUCE PHASE (R1)
                .reduceByKey(lambda x, y: x + y) # <-- REDUCE PHASE (R2)
    )

    valA = 0
    valB = 0

    for point in distance.collect():
        group , dist = point[0], point[1]
        if group == 0:
            valA = dist
        if group == 1:
            valB = dist

    return max(valA/NA, valB/NB)


def MRPrintStatistics(U, C):

    """
    MapReduce
    -MAP PHASE R1 (flatMap): retrieve the centroid each point belongs to and map an RDD of (centroid index, point) pairs
    -REDUCE PHASE R1 (mapPartitions): for each partition associate the sum of A points and B points contained in that partition with each centroid
    -SHUFFLE (groupByKey): output pairs of the type (centroid index, <list of iterables>) where <list of iterables> is the list of the partial sums
            (of A points and B points associated with that centroid) coming up from different partitions
    -REDUCE PHASE R2  (flatMap): for each centroid gather partial sums and return an RDD where each element is of the type
                                (centroid, (sum of A points, sum of B points))
    """

    stats = (U.flatMap(lambda point: [calc_center(parse_coors_group(point), C)])    # <-- MAP PHASE (R1)
            .mapPartitions(count_points)                                                            # <-- REDUCE PHASE (R1)
            .groupByKey()                                                                           # <-- SHUFFLE+GROUPING
            .flatMap(gather_partial_sums)                                                           # <-- REDUCE PHASE (R2)
    )

    sorted_triplets = stats.sortByKey().collect()         # collect() action on stats sorted RDD

    for t in sorted_triplets:
        print(f"i = {t[0]}, center = {tuple(C[t[0]])}, NA = {t[1][0]}, NB = {t[1][1]}\t\t\t")

    return


def main():
    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 5, "Write the command line G16HW1.py <file_name> <L> <K> <M>"

    # SPARK SETUP
    conf = SparkConf().setAppName('G16HW1')
    sc = SparkContext(conf=conf)

    # DATA FROM COMMAND LINE
    file_path = sys.argv[1]
    assert os.path.isfile(file_path), "File or folder not found"

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
    print("Input file =", file_path, ", L =", L, ", K =", K, ", M =", M)

    # READ THE INPUT POINTS INTO AN RDD OF (point, group) PAIRS, SUBDIVIDED INTO L PARTITIONS
    inputPoints = sc.textFile(file_path).repartition(numPartitions = L).cache()

    # PRINTS THE NUMBER N OF POINTS, THE NUMBER NA OF POINTS OF A GROUP A, AND NUMBER NB OF POINTS OF GROUP B
    A = inputPoints.filter(lambda x: x[-1] == 'A')
    B = inputPoints.filter(lambda x: x[-1] == 'B')

    global NA, NB
    NA = A.count()
    NB = B.count()

    print("N =", inputPoints.count(), ", NA = ", NA , ", NB = ", NB)

    # COMPUTES A SET C OF K CENTROIDS BY USING THE SPARK IMPLEMENTATION OF THE STANDARD LLOYD'S ALGORITHM FOR THE INPUT POINTS, DISREGARDING THE POINTS' DEMOGRAPHIC GROUPS, AND USING M AS NUMBER OF ITERATIONS
    points = inputPoints.map(lambda x: Vectors.dense(parse_coors_group(x)[0]))
    kmeans = KMeans.train(points, k = K, maxIterations = M, initializationMode = "k-means||")
    C = kmeans.clusterCenters

    # PRINTS THE VALUES OF THE TWO OBJECTS FUNCTIONS ->  MRComputeStandardObjective and MRComputeFairObjective
    print("Delta(U, C) =", MRComputeStandardObjective(inputPoints, C))
    print("Phi(A,B,C) =", MRComputeFairObjective(inputPoints, C))

    # FOR EACH CLUSTER COMPUTE AND PRINT TRIPLETS CONTAINING STATS (CENTROID, NUMBER OF POINTS A LABELED, NUMBER OF POINTS B LABELED)
    MRPrintStatistics(inputPoints, C)


if __name__ == "__main__":
    main()
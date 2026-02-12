from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random

# GLOBAL VARIABLES
p = 8191

# CREATE HASH FUNCTION h
def generate_list_hash_function_h(list_a, list_b, C):
    list_hash = []
    for i in range(0, len(list_a)):
        A = list_a[i]
        B = list_b[i]
        hash_fun = lambda x, a=A, b=B: ((a * x + b) % p) % C
        list_hash.append(hash_fun)
    return list_hash

# CREATE HASH FUNCTION g
def generate_list_hash_function_g(list_a, list_b):
    list_hash = []
    for i in range(0, len(list_a)):
        A = list_a[i]
        B = list_b[i]
        hash_fun = lambda x, a=A, b=B: (2*(((a * x + b) % p) % 2) - 1)
        list_hash.append(hash_fun)
    return list_hash


def heavy_hitters(histogram, K):
    histogram_sort = sorted(histogram.items(), key=lambda item: item[1],
                            reverse=True)  # sort histogram in descending order

    top_K_items = dict(histogram_sort[:K])
    freq_K = histogram_sort[K-1][1] # element of highest frequency is the last in the sorted histogram
    i = K
    while i < len(histogram_sort) and histogram_sort[i][1] == freq_K:
        item, freq = histogram_sort[i]
        top_K_items[item] = freq
        i += 1

    return top_K_items

def heavy_hitters_stats(histogram, K):
    top_K_items = heavy_hitters(histogram, K)
    print(f"Number of Top-K Heavy Hitters = {len(top_K_items)}")

    # AVERAGE ERRORS
    avg_cm_error, avg_cs_error = average_error(top_K_items)
    threshold = 1e-2
    format_cond_cm = ""
    if abs(avg_cm_error) >= threshold:
        format_cond_cm = ".15f"
    else:
        format_cond_cm = ".15E"

    print(f"Avg Relative Error for Top-K Heavy Hitters with CM = {avg_cm_error:{format_cond_cm}}")

    format_cond_cs = ""
    if abs(avg_cs_error) >= threshold:
        format_cond_cs = ".15f"
    else:
        format_cond_cs = ".15E"

    print(f"Avg Relative Error for Top-K Heavy Hitters with CS = {avg_cs_error:{format_cond_cs}}")

    # TOP-K HEAVY HITTERS TRUE FREQUENCIES, TOP-K HEAVY HITTERS CM ESTIMATED FREQUENCIES
    if K <= 10:
        print("Top-K Heavy Hitters:")
        print_heavy_hitters_freqs(top_K_items)


def print_heavy_hitters_freqs(top_K_items):
        top_K_items_sorted = dict(
            sorted(top_K_items.items(), key=lambda item: item[0]))  # sort top_K_items in increasing order
        for item in top_K_items_sorted.keys():
            true_freq = top_K_items_sorted[item]
            est_freq = float('inf')
            for j in range(D):
                freq_temp = CM[j][hash_func_h_list[j](item)]
                if est_freq > freq_temp:
                    est_freq = freq_temp

            print(f"Item {item} True Frequency = {true_freq} Estimate Frequency with CM = {est_freq}")

def average_error(top_K_items):

    # average error for cm
    error_cm = 0
    for item in top_K_items:
        true_freq = top_K_items[item]

        est_freq = float('inf')
        for j in range(D):
            freq_temp = CM[j][hash_func_h_list[j](item)]
            if est_freq > freq_temp:
                est_freq = freq_temp
        error_cm += (abs(true_freq - est_freq) / true_freq)

    # average error of cs
    error_cs = 0
    for item in top_K_items:
        true_freq = top_K_items[item]

        est_freq_list = []
        for j in range(D):
            freq_temp = CS[j][hash_h_list_cs[j](item)] * hash_g_list_cs[j](item)
            est_freq_list.append(freq_temp)

        est_freq_list.sort()
        est_freq = est_freq_list[D // 2]                 # calculate median of estimated frequencies
        error_cs += (abs(true_freq - est_freq) / true_freq)

    avg_cm_error = error_cm/len(top_K_items)
    avg_cs_error = error_cs/len(top_K_items)

    return avg_cm_error, avg_cs_error


# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, histogram, CM, CS, hash_func_h_list, hash_h_list_cs, hash_g_list_cs
    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0] >= T:
        return

    streamLength[0] += batch_size
    # Extract the distinct items from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: i1 + i2).collectAsMap()

    # Update the streaming state
    for key in batch_items:
        if key not in histogram:
            histogram[key] = 0
        histogram[key] += batch_items[key] # iterate the frequency of the value

        # count-min sketch
        for i in range(D):
            col = hash_func_h_list[i](key)
            CM[i][col] += batch_items[key]

        # count sketch
        for i in range(D):
            col = hash_h_list_cs[i](key)
            g_sign = hash_g_list_cs[i](key)
            CS[i][col] += batch_items[key] * g_sign

    if streamLength[0] >= T:
        stopping_condition.set()


if __name__ == '__main__':
    assert len(sys.argv) == 6, "Write the command line G16HW3.py <portExp> <T> <D> <W> <K>"

    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("G16HW3")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

    # Here, with the duration you can control how large to make your batches.
    # Beware that the data generator we are using is very fast, so the suggestion
    # is to use batches of less than a second, otherwise you might exhaust the memory.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")

    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.
    stopping_condition = threading.Event()

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    portExp = sys.argv[1]
    assert portExp.isdigit(), "portExp must be an integer"
    portExp = int(portExp)

    T = sys.argv[2]
    assert T.isdigit(), "T must be an integer"
    T = int(T)

    D = sys.argv[3]
    assert D.isdigit(), "D must be an integer"
    D = int(D)

    W = sys.argv[4]
    assert W.isdigit(), "W must be an integer"
    W = int(W)

    K = sys.argv[5]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0]
    histogram = {}  # Hash Table

    # functions h -> to implement in count min sketch and count sketch
    hlist_a = random.sample(range(1, p), D)
    hlist_b = random.sample(range(0, p), D)
    hash_func_h_list = generate_list_hash_function_h(hlist_a, hlist_b, W)

    hlist_a = random.sample(range(1, p), D)
    hlist_b = random.sample(range(0, p), D)
    hash_h_list_cs = generate_list_hash_function_h(hlist_a, hlist_b, W)
    glist_a = random.sample(range(1, p), D)
    glist_b = random.sample(range(0, p), D)
    hash_g_list_cs = generate_list_hash_function_g(glist_a, glist_b)


    CM = [[0] * W for _ in range(D)] # initialization of CM
    CS = [[0] * W for _ in range(D)] # initialization of CS

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))

    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")

    # The following command stops the execution of the stream. The first boolean, if true, also
    # stops the SparkContext, while the second boolean, if true, stops gracefully by waiting for
    # the processing of all received data to be completed. You might get some error messages when the
    # program ends, but they will not affect the correctness.

    ssc.stop(False, False)
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    print(f"Port = {portExp}, T = {T}, D = {D}, W = {W}, K = {K}")
    print("Number of items processed =", streamLength[0])
    print("Number of distinct items =", len(histogram))

    # HEAVY HITTERS STATS (AVERAGE ERRORS, TOP-K HEAVY HITTERS TRUE FREQUENCIES, TOP-K HEAVY HITTERS CM ESTIMATED FREQUENCIES)
    heavy_hitters_stats(histogram, K)
# Homework 3

In this homework we use the Spark Streaming API to devise a program which processes a stream of items and compares the effectiveness of the count-min and count sketches to estimate the individual frequencies of heavy hitters.  

#### INPUT values
The program *G16HW3.py* receives in input the following 5 command-line arguments (in the given order):  
- an integer portExp: the port number;  
- an integer T: the target number of items to process;  
- an integer D: the number of rows of each sketch;  
- an integer W: the number of columns of each sketch;  
- an integer K: the number of top frequent items of interest.

#### Execution
*G16HW3.py* computes the following information:  
- The exact frequencies of all distinct items of Σ;
- A D×W count-min sketch CM for Σ. We implemented the D hash functions hj, with 0≤j<D, which can generate randomly from the family;
- A D×W count sketch CS for Σ. For this sketch it is needed 2D hash functions, hj and gj, with 0≤j<D, which can generate randomly from the family. 
- The average relative error of the frequency estimates provided by CM for the top-K heavy hitters. The top-K heavy hitters are defined as the items of u∈Σ whose true frequency is fu≥ϕ(K), where ϕ(K) is the K-th value in the list of frequencies of the items of Σ, sorted in non-increasing order.
E.g., if the true frequencies are 11, 10, 10, 9, 9, 9, 8, 8, 6 6, and K=5, then ϕ(K)=9.
Important: the number of top-K heavy hitters might be larger than K, which is a typical scenario for top-K queries. If fu is the estimated frequency for u, the relative error of this estimate is |fu−fu|/fu.
- The average relative error of the frequency estimates provided by CS for the top-K heavy hitters, were the top-K heavy hitters are defined exactly as in the previous point.

#### OUTPUT results
The program prints:  
- The input parameters provided as command-line arguments
- The number of processed items, that is, |Σ|;
- The number of distinct items in Σ;
- The average relative error of the frequency estimates provided by CM for the top-K heavy hitters;
- The average relative error of the frequency estimates provided by CS for the top-K heavy hitters;
- (Only if K≤10) True and estimated frequencies of the top-K heavy hitters, sorted in increasing order of item. For the estimated frequencies consider only those of CM.

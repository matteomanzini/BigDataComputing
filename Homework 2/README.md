# Homework 2

The purpose of the second homework is to implement the variant of Lloyd's algorithm for fair k-means clustering and to compare its effectiveness against the standard variant, with respect to the new objective function introduced in that paper.

In the code we have implemented three algorithms:

#### 1. *MRFairLloyd()* function

The method takes in input an RDD representing a set U of points, with demographic group labels, and two parameters  K,M, (integers), computing the following steps:

- Initializes a set C of K centroids using kmeans.
- Executes M iterations of the above repeat-until loop.
- Returns the final set C of centroids.

The set C must be represented as an array of tuples in Python.

#### 2. Include function *MRComputeFairObjective()* from Homework 1 directory

#### 3. G16HW2.py

The file receives in input as command-line arguments a path to the file storing the input points, and 3 integers L,K,M, and does the following:

- Prints the command-line arguments and stores  L,K,M into suitable variables.
- Reads the input points into an RDD -called inputPoints-, subdivided into L partitions.
- Prints the number N of points, the number NA of points of group A, and the number NB of points of group B (hence, N=NA+NB). 
- Computes a set Cstand of K centroids for the input points, by running the Spark implementation of the standard Lloyd's algorithm,  with M iterations, disregarding the demographic groups.
- Computes a set Cfair of K centroids by running MRFairLloyd(inputPoints,K,M).
- Computes and prints Φ(A,B,Cstand) and Φ(A,B,Cfair).
- Prints separately the times, in milliseconds, spent to compute : Cstand, Cfair, Φ(A,B,Cstand) and Φ(A,B,Cfair).


#### 4. Program G16GEN.py

The program receives in input two integers N, K and generates a dataset of N points to maximize the gap between the solutions provided by the standard LLoyd's algorithm and its fair variant.  
The program must print the points and their respective demographic groups in output, using the same format as for the input points. A short description of the generator must be given in the word file used to report the experiments on the cluster.  

The solutions implemented to obtain the expected results is explaned in the file *Homework 2/G16HW2form.pdf* .
Ideally, we generate clusters of B points and inside of each cluster, another small one of A points. The points are randomly and uniformly inside of each circle.  
A points are will be less than half of B points.  
The idea of making such clusters is to highlight the differences between Standard K-means clustering and Fair K-means perfomed on te same dataset.  
A and B points are likely to unbalanced with each cluster and the *MRComputeFairObjective()* score is poor.  

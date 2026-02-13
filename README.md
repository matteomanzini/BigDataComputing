# BigDataComputing

# Homework 1

The object of this homework is to get familiar with Apache Spark and with MapReduce algorithms.  
The homework concerns a variant of the classical Lloyd's algorithm for k-means clustering, which enforces a fairness constraint on the solution based on extra demographic information attached to the input points.  
There are three algorithms that have to implement:

##### 1. K-MEANS CLUSTERING

Given a set of points U ⊂ R^D and an integer K,  k-means clustering aims at determining a set C ⊂ R^D of K centroids which minimize the objective function defined in the code.  
Since the problem is NP-hard, current efficient solutions seek approximate solutions and Lloyd's algorithm is widely used to this purpose.  
The solutions proposed is defined as:
<pre>
  Compute an initial set C={c1,c2,…,cK} of centroids
  Repeat M times
  └── Partition U into K clusters U1,U2,⋯,UK, where Ui consists of the points of U whose closest centroid is ci
      For 1≤i≤K, compute a new centroid ci as the average of the points of Ui
</pre>

We write a function MRComputeStandardObjective that takes in input the set U=A∪B and a set C of centroids, and returns the value of the objective function ignoring the demographic groups.

##### 2. FAIR K-MEANS CLUSTERING

It is a fair variant of Lloyd's algorithm such that the input set of points U is split into two demographic groups A,B. The goal of this this version is to minimize the objective function Φ(A,B,C) defined in the code.  
The only difference concerns in the last step of previous pseudocode, where the computation of the new centroids c1,c2,…,cK from the current partition U1,U2,…,UK is performed through a gradient descent protocol.  

We write a function MRComputeFairObjective that takes in input the set U=A∪B and a set C of centroids, and returns the value of the objective function Φ(A,B,C).

##### 3. MRPrintStatistics function

MRPrintStatistics takes in input the set U=A∪B and a set C of centroids, and computes and prints the triplets (ci,NAi,NBi), for 1≤i≤K=|C|, where ci is the i-th centroid in C, and NAi,NBi are the numbers of points of A and B, respectively, in the cluster Ui centered in ci.  

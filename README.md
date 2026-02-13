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

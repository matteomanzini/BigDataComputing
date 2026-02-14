# Homework 2

The purpose of the second homework is to implement the variant of Lloyd's algorithm for fair k-means clustering and to compare its effectiveness against the standard variant, with respect to the new objective function introduced in that paper.

In the code we have implemented three algorithms:

### 1. MRFairLloyd function

The method takes in input an RDD representing a set U of points, with demographic group labels, and two parameters  K,M, (integers), computing the following steps:

- Initializes a set C of K centroids using kmeans.
- Executes M iterations of the above repeat-until loop.
- Returns the final set C of centroids.

The set C must be represented as an array of tuples in Python.

### 2. Include function MRComputeFairObjective from Homework 1 directory

### 3. G16HW2.py

# RP-DBSCAN: A Superfast Parallel DBSCAN Algorithm Based on Random Partitioning
##1. Overview
Following the recent trends in big data processing, several parallel DBSCAN algorithms have been reported in the literature. In most such algorithms, neighboring points are assigned to the same data partition for parallel processing to facilitate calculation of the density of the neighbors. This data partitioning scheme causes a few critical problems including load imbalance between data partitions, especially in a skewed data set. To remedy these problems, we propose a cell-based data partitioning scheme, pseudo random partitioning, that randomly distributes small cells rather than the points themselves. It achieves high load balance regardless of data skewness while retaining the data contiguity required for DBSCAN. In addition, we build and broadcast a highly compact summary of the entire data set, which we call a two-level cell dictionary, to supplement random partitions. Then, we develop a novel parallel DBSCAN algorithm, Random Partitioning-DBSCAN (shortly, RPDBSCAN), that uses pseudo random partitioning together with a two-level cell dictionary. The algorithm simultaneously finds the local clusters to each data partition and then merges these local clusters to obtain global clustering. To validate the merit of our approach, we implement RP-DBSCAN on Spark and conduct extensive experiments using various real-world data sets on 12 Microsoft Azure machines (48 cores). In RP-DBSCAN, data partitioning and cluster merging are very light, and clustering on each split is not dragged out by a specific worker. Therefore, the performance results show that RP-DBSCAN significantly outperforms the state-of-the-art algorithms by up to 180 times.

##2. Algorithms
- DBSCAN [1] : 
- SPARK-DBSCAN [2] : 
- ESP-DBSCAN [3] : 
- RBP-DBSCAN [4] : 
- CBP-DBSCAN [5] : 
- NG-DBSCAN [6] : 

##3. Data Sets
| Name           | # Object       | # Dim    | Size    | Type  |  Link   |
| :------------: | :------------: | :------: |:-------:|:-----:|:-------:|
| GeoLife        | 24,876,978     | 3        | 808 MB  | float |         |
| Cosmo50        | 315,086,245    | 3        | 11.2 GB | float |         |
| OpenStreetMap  | 2,770,238,904  | 2        | 77.1 GB | float |         |
| TeraClickLog   | 4,373,472,329  | 13       | 362 GB  | float |         |

##4. Configuration

##5. How to run

##6. Example

##7. Experiment

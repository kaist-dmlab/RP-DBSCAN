# RP-DBSCAN: A Superfast Parallel DBSCAN Algorithm Based on Random Partitioning
> __Publication__ </br>
> Song, H. and Lee, J., "RP-DBSCAN: A Superfast Parallel DBSCAN Algorithm Based on Random Partitioning," *In Proc. 2018 ACM Int'l Conf. on Management of Data (SIGMOD)*, Houston, Texas, June 2018. 


## 1. Overview
Following the recent trends in big data processing, several parallel DBSCAN algorithms have been reported in the literature. In most such algorithms, neighboring points are assigned to the same data partition for parallel processing to facilitate calculation of the density of the neighbors. This data partitioning scheme causes a few critical problems including load imbalance between data partitions, especially in a skewed data set. To remedy these problems, we propose a cell-based data partitioning scheme, pseudo random partitioning, that randomly distributes small cells rather than the points themselves. It achieves high load balance regardless of data skewness while retaining the data contiguity required for DBSCAN. In addition, we build and broadcast a highly compact summary of the entire data set, which we call a two-level cell dictionary, to supplement random partitions. Then, we develop a novel parallel DBSCAN algorithm, Random Partitioning-DBSCAN (shortly, RPDBSCAN), that uses pseudo random partitioning together with a two-level cell dictionary. The algorithm simultaneously finds the local clusters to each data partition and then merges these local clusters to obtain global clustering. To validate the merit of our approach, we implement RP-DBSCAN on Spark and conduct extensive experiments using various real-world data sets on 12 Microsoft Azure machines (48 cores). In RP-DBSCAN, data partitioning and cluster merging are very light, and clustering on each split is not dragged out by a specific worker. Therefore, the performance results show that RP-DBSCAN significantly outperforms the state-of-the-art algorithms by up to 180 times.

## 2. Algorithms
- DBSCAN [1] 
- SPARK-DBSCAN [2] 
- ESP-DBSCAN [3] 
- RBP-DBSCAN [4] 
- CBP-DBSCAN [2] 
- NG-DBSCAN [6] 
- **RP-DBSCAN** : Spark implementation of our algorithm

>__*Reference*__</br>
[1] Martin Ester, Hans-Peter Kriegel, J¨org Sander, and Xiaowei Xu. 1996. A Density-Based Algorithm for Discovering Clusters in Large Spatial Databases with Noise. *In Proc. 2nd Int’l Conf. on Knowledge Discovery and Data Mining*. 226–231.</br>
[2] Yaobin He, Haoyu Tan, Wuman Luo, Shengzhong Feng, and Jianping Fan. 2014. MR-DBSCAN: A Scalable MapReduce-based DBSCAN Algorithm for Heavily Skewed Data. *Frontiers of Computer Science* 8, 1 (2014), 83–99.</br>
[3] Irving Cordova and Teng-Sheng Moh. 2015. DBSCAN on Resilient Distributed Datasets. *In Proc. 2015 Int’l Conf. on High Performance Computing & Simulation*. 531–540.</br>
[4] Bi-Ru Dai and I-Chang Lin. 2012. Efficient Map/Reduce-Based DBSCAN Algorithm with Optimized Data Partition. *In Proc. 2012 IEEE Int’l Conf. on Cloud Computing*. 59–66.</br>
[5] Alessandro Lulli, Matteo Dell’Amico, Pietro Michiardi, and Laura Ricci. 2016. NG-DBSCAN: Scalable Density-Based Clustering for Arbitrary Data. *Proceedings of the VLDB Endowment* 10, 3 (2016), 157–168.

## 3. Data Sets
| Name           | # Object       | # Dim    | Size    | Type  |  Link   |
| :------------: | :------------: | :------: |:-------:|:-----:|:-------:|
| GeoLife        | 24,876,978     | 3        | 808 MB  | float | [link](http://www.microsoft.com/en-us/download/) |
| Cosmo50        | 315,086,245    | 3        | 11.2 GB | float | [link](http://nuage.cs.washington.edu/benchmark/astro-nbody/) |
| OpenStreetMap  | 2,770,238,904  | 2        | 77.1 GB | float | [link](http://blog.openstreetmap.org/2012/04/01/bulk-gps-point-data/) |
| TeraClickLog   | 4,373,472,329  | 13       | 362 GB  | float | [link](http://labs.criteo.com/downloads/download-terabyte-click-logs/) |

## 4. Configuration
 - We conducted experiments on 12 Microsoft Azure D12v2 instances loacted in South Korea. 
 - Each instance has four cores, 28GB of main memory, and 200GB of disk (SSD). 
 - All instances run on Ubuntu 16.04.3 LTS. We used Spark 2.1.0 for distributed parallel processing. 
 - Ten out of 12 instances were used as worker nodes, and the remaining two instances were used as master nodes. 
 - RP-DBSCAN algorithm was written in the Java programming language and run on JDK 1.8.0_131.

## 5. How to run
- Compile.
  - Download the spark library from [Apache Spark](http://spark.apache.org/downloads.html).
  - Make a _jar file_ using IDE tools. For example, you can easily make it using Eclipse through *project name->export->jar file*. It is possible that you just download the jar file in [Jar](Jar) folder.
- Create _Azure HDInsight_ instances.
  - Refer to [HDInsight Document](https://docs.microsoft.com/en-us/azure/hdinsight/).
- Move the data sets into the _HDFS_.
  - Download all data sets from the above links and move them to the _Azure master node_.
  - Transfer your data sets from the _Azure master node_ into _HDFS_.</br>
    ```
    hadoop dfs -put localPathForInputData hdfsPathForInputData
    ```
- Run **RP-DBSCAN** algorithm.
  - Necessary algorithm parameters.
    ```
    -i : the hdfs path for input data set.
    -o : the local path to write the meta result of clustering.
    -np : the total number of cpu cores ( or partitions which you want to set ).
    -rho : the approximation parameter
    -dim : the number of dimensions.
    -minPts : the minimum number of neighbor points.
    -eps : the radius of a neighborhood.
    ```
  - Optional algorithm parameters.
    ```
    -bs : the block size for virtually combining two-level cell dictionary. (default : 1)");
    -l : the hdfs path to write labeled points, <point id, cluster label>. (default : no output)");
    ```  
  - Execution commend.
    ```
    spark-submit --class mainClass jarFile -i hdfsInputPath -o localOutputPath -np numOfPartitions -rho rhoValue -dim numOfDimensions -eps epsilonValue -minPts minPtsValue
    ```
  
 
## 6. Example


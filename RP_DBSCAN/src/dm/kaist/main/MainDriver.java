package dm.kaist.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import dm.kaist.algorithm.Conf;
import dm.kaist.algorithm.PARALLEL_DBSCAN;
import dm.kaist.graph.Edge;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.FileIO;
import dm.kaist.io.SerializableConfiguration;
import dm.kaist.meta.ApproximatedCell;
import dm.kaist.partition.Partition;
import scala.Tuple2;

public class MainDriver {
	/**
	 * @author Hwanjun Song(KAIST), Jae-Gil Lee(KAIST)
	 * Created on 18/02/07
	 * To find clusters using RP-DBSCAN
	 **/
	public static void main(String[] args) throws IOException, ClassNotFoundException
	{
		//Parameter Load
		Conf.setInputParameters(args);
		
		//You should change spark configurations to achieve the best performance with considering your system environment.
		SparkConf sparkConf = Conf.setSparkConfiguration("13", "4", "20g", "10g", "2048");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SerializableConfiguration conf = new SerializableConfiguration();
		
		//Refresh Folder and Files
		FileIO.refreshFolder(conf);
		
		long start = 0, end = 0;
		start = System.currentTimeMillis();
		
		/**
		 * Phase I-1: File Read & Pseudo Random Partitioning
		 */
		JavaRDD<String> lines = sc.textFile(Conf.inputPath, Conf.numOfPartitions);
		JavaPairRDD<List<Integer>, ApproximatedCell> dataMap = null;
		if(Conf.boost)
		{
			dataMap = lines.mapToPair(new PARALLEL_DBSCAN.PointToCell(Conf.dim, Conf.epsilon))
			.combineByKey(new PARALLEL_DBSCAN.CreateLocalApproximatedPoint(Conf.dim, Conf.epsilon, Conf.rho), new PARALLEL_DBSCAN.LocalApproximation(Conf.dim, Conf.epsilon, Conf.rho), new PARALLEL_DBSCAN.GlobalApproximation(Conf.dim))
			.mapToPair(new PARALLEL_DBSCAN.PseudoRandomPartition2(Conf.metaBlockWindow)).persist(StorageLevel.MEMORY_AND_DISK_SER());
		}else
			dataMap = lines.mapToPair(new PARALLEL_DBSCAN.PointToCell(Conf.dim, Conf.epsilon)).groupByKey().mapToPair(new PARALLEL_DBSCAN.PseudoRandomPartition(Conf.dim, Conf.epsilon, Conf.rho, Conf.metaBlockWindow)).persist(StorageLevel.MEMORY_AND_DISK_SER());

		System.out.println("# of Level 1 Cells : " + dataMap.count());
		
		/**
		 * Phase I-2: Build Two-Level Cell Dictionary with Dictionary Defragmentation
		 */
		// Dictionary Defragmentation
		JavaPairRDD<List<Integer>, Long> ptsCountforEachMetaBlock = dataMap.mapToPair(new PARALLEL_DBSCAN.MetaBlockMergeWithApproximation()).reduceByKey(new PARALLEL_DBSCAN.AggregateCount());
		List<Tuple2<List<Integer>, Long>> numOfPtsInCell = ptsCountforEachMetaBlock.collect();
		System.out.println("# of Blocks for virtually combining : " + numOfPtsInCell.size());
		
		HashMap<List<Integer>,List<Integer>> partitionIndex = new HashMap<List<Integer>,List<Integer>>();
		List<Partition> wholePartitions = PARALLEL_DBSCAN.scalablePartition(numOfPtsInCell, Conf.dim, Conf.numOflvhCellsInMetaPartition/Conf.dim, partitionIndex);
		System.out.println("# of contiguous sub-dictionaries : " + wholePartitions.size());	
		
		// Build Two-Level Cell Dictionary which composed of multiple sub-dictionaries
		JavaPairRDD<Integer, Iterable<ApproximatedCell>> evenlySplitPartitions = dataMap.flatMapToPair(new PARALLEL_DBSCAN.AssignApproximatedPointToPartition(partitionIndex)).groupByKey(wholePartitions.size());
		JavaPairRDD<Null, Null> metaDataSet = evenlySplitPartitions.mapToPair(new PARALLEL_DBSCAN.MetaGenerationWithApproximation(Conf.dim, Conf.epsilon, Conf.rho, Conf.minPts, conf, wholePartitions));
		metaDataSet.collect();
		
		
		// Re-partition the pseudo random partitions into Each Worker by a randomly assigned integer value
		JavaPairRDD<Integer, ApproximatedCell> dataset = dataMap.mapToPair(new PairFunction<Tuple2<List<Integer>,ApproximatedCell>, Integer, ApproximatedCell>() {
			@Override
			public Tuple2<Integer, ApproximatedCell> call(Tuple2<List<Integer>, ApproximatedCell> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, ApproximatedCell>((int)(Math.random()*Conf.numOfPartitions), arg0._2);
			}
		}).repartition(Conf.numOfPartitions).persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		// Broadcast two-level cell dictionary to every workers.
		List<String> metaPaths = FileIO.broadCastData(sc, conf, Conf.metaFoler);
		
		/**
		 * Phase II-1: Core Marking
		 */
		JavaPairRDD<Long, ApproximatedCell> coreCells = dataset.mapPartitionsToPair(new PARALLEL_DBSCAN.FindCorePointsWithApproximation(Conf.dim, Conf.epsilon, Conf.minPts, conf, metaPaths)).persist(StorageLevel.MEMORY_AND_DISK_SER());
		List<Tuple2<Integer, Long>> numOfCores = coreCells.mapToPair(new PairFunction<Tuple2<Long,ApproximatedCell>, Integer, Long>() {

			@Override
			public Tuple2<Integer, Long> call(Tuple2<Long, ApproximatedCell> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Long>(1, (long)arg0._2.getRealPtsCount());
			}
		
		}).reduceByKey(new PARALLEL_DBSCAN.AggregateCount()).collect();
		System.out.println("# of core : " + numOfCores.get(0)._2);
		
		
		//Broadcast core cell ids to every workers
		List<String> corePaths =  FileIO.broadCastData(sc, conf, Conf.coreInfoFolder);

		/**
		 * Phase II-2: Cell Graph Construction
		 */
		JavaPairRDD<Integer, Edge> edgeSet = coreCells.mapPartitionsToPair(new PARALLEL_DBSCAN.FindDirectDensityReachableEdgesWithApproximation(Conf.dim, Conf.epsilon, Conf.minPts, conf, metaPaths, corePaths ,Conf.numOfPartitions)).repartition(Conf.numOfPartitions/2);
		
		/**
		 * Phase III-1: Tournament Merging
		 */
		int curPartitionSize = Conf.numOfPartitions;
		
		while(curPartitionSize != 1)
		{
			curPartitionSize = curPartitionSize/2;
			edgeSet = edgeSet.mapPartitionsToPair(new PARALLEL_DBSCAN.BuildMST(conf, corePaths, curPartitionSize)).repartition(curPartitionSize);
		}

		List<Tuple2<Integer, Integer>> result = edgeSet.mapPartitionsToPair(new PARALLEL_DBSCAN.FinalPhase(conf, corePaths)).collect();

		int numOfCluster = result.get(0)._2;
		System.out.println("# of clusters : " + numOfCluster);

		/**
		 * Phase III-2: Point Labeling
		 */
		JavaPairRDD<Integer, ApproximatedPoint> borderPts = dataset.flatMapToPair(new PARALLEL_DBSCAN.EmitConnectedCoreCellsFromBorderCell(conf, Conf.numOfPartitions)).groupByKey().flatMapToPair(new PARALLEL_DBSCAN.AssignBorderPointToCluster(Conf.dim, Conf.epsilon, conf, Conf.outputPath));
		JavaPairRDD<Integer, ApproximatedPoint> corePts = dataset.mapPartitionsToPair(new PARALLEL_DBSCAN.AssignCorePointToCluster(conf, Conf.outputPath));
		
		//labeling algorithm 1 : faster than algorithm 2, but not scalable.
		//If out-of-memory error is occurred during the labeling procedure, then use below algorithm 2 for labeling instead of this.
		JavaPairRDD<Integer, ApproximatedPoint> assignedResult = borderPts.union(corePts);
		List<Tuple2<Integer, Long>> counts = assignedResult.mapPartitionsToPair(new PARALLEL_DBSCAN.CountForEachCluster()).reduceByKey(new PARALLEL_DBSCAN.AggregateCount()).collect();
		int clusterId = 1;
		for(Tuple2<Integer, Long> cluster : counts)
			System.out.println("CLUSTER ["+(clusterId++)+"] : "+ cluster._2);
		 
		/*
		// labeling algorithm 2
		List<Tuple2<Integer, Long>> borderPtsList =  borderPts.mapPartitionsToPair(new PARALLEL_DBSCAN.CountForEachCluster()).reduceByKey(new PARALLEL_DBSCAN.AggregateCount()).collect();
		List<Tuple2<Integer, Long>> corePtsList =  corePts.mapPartitionsToPair(new PARALLEL_DBSCAN.CountForEachCluster()).reduceByKey(new PARALLEL_DBSCAN.AggregateCount()).collect();
		
		HashMap<Integer, Long> numOfPtsInCluster = new HashMap<Integer, Long>();
		for(Tuple2<Integer, Long> core : corePtsList)
			numOfPtsInCluster.put(core._1, core._2);
		for(Tuple2<Integer, Long> border : borderPtsList)
			numOfPtsInCluster.put( border._1 , numOfPtsInCluster.get(border._1)+border._2);

		for(Entry<Integer, Long> entry : numOfPtsInCluster.entrySet())
			System.out.println("CLUSTER ["+(entry.getKey()+1)+"] : "+ entry.getValue());
		*/

		sc.close();
		
		end = System.currentTimeMillis();
		System.out.println("Total Elapsed Time : " + (end-start)/1000 +" s");
	}
	

}

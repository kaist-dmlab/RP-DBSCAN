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
import dm.kaist.algorithm.Methods;
import dm.kaist.algorithm.RP_DBSCAN;
import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.graph.Edge;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.FileIO;
import dm.kaist.io.SerializableConfiguration;
import dm.kaist.partition.Partition;
import scala.Tuple2;

public class MainDriver {
	/**
	 * @author Hwanjun Song(KAIST), Jae-Gil Lee(KAIST)
	 * Created on 18/03/02
	 * To find clusters using RP-DBSCAN
	 **/
	public static void main(String[] args) throws IOException, ClassNotFoundException
	{
		//Parameter Load
		Conf.setInputParameters(args);
		
		//You should change spark configurations to achieve the best performance with considering your system environment.
		SparkConf sparkConf = Conf.setSparkConfiguration("5", "4", "20g", "10g", "2048");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		long start, end;
		start = System.currentTimeMillis();
		
		RP_DBSCAN rp_dbscan = new RP_DBSCAN(sc);
		
		//PHASE I: Data Partitioning
		rp_dbscan.phaseI();
		
		//PHASE II: Cell Graph Construction
		rp_dbscan.phaseII();
		
		//PHASE III: Cell Graph Merging
		rp_dbscan.phaseIII();
		
		end = System.currentTimeMillis();
		
		//Write meta results
		rp_dbscan.writeMetaResult((end-start));
		
	}
	

}

package dm.kaist.algorithm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.spark.SparkConf;

import dm.kaist.graph.Edge;
import dm.kaist.graph.LabeledCell;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;
import dm.kaist.meta.ApproximatedCell;
import dm.kaist.meta.Cell;

public final class Conf  implements Serializable{
	
	//input parameters
	public static String inputPath = null;
	public static int numOfPartitions = 0;
	public static float rho = 0;
	public static int dim = 0;
	public static int minPts = 0;
	public static float epsilon = 0;
	public static boolean skipping = true;
	public static boolean tournament = true;
	public static int metaBlockWindow = 1;
	public static boolean boost = false;
	
	//the flag for writing the results of labeling
	public static boolean writeLabelingResult = false;
	
	//additional parameters
	public static String delimeter = " ";
	public static String metaFoler = "DICTIONARY";
	public static String convertTableFolder = "CONVERTS";
	public static String coreInfoFolder = "CORE_INFO";
	public static String metaResult = "META_RESULT";
	public static String labelingResult = "LABELING";
	public static int limitDimForVirtualCombining = 6;
	public static int numOflvhCellsInMetaPartition = 12000000;
	public static int limitNumOflv1Cell = 1000000;
	
	public static void setInputParameters(String[] args)
	{
		if(args.length < 6 || args.length > 8)
		{
			System.out.println("Usage: <inputPath> <numOfPartitions> <rho> <dim> <minPtr> <epsilon> <skipping> <tournament> <optional : metaBlockWindow> <optional : boost>");
			System.out.println("-inputPath : the path for input dataset.");
			System.out.println("-numOfPartitions : the number of cores or partitions which you want to set.");
			System.out.println("-rho : approximation rate.");
			System.out.println("-dim : dimension of dataset.");
			System.out.println("-minPtr : minumum number of points in neighborhood to be core point.");
			System.out.println("-epsilon : region query boundary.");
			System.out.println("-metaBlockWindow : block size for virtually combining two-level cell dictionary. (default : 1)");
			System.exit(1);
		}
		
		if(args.length > 5)
		{
			inputPath = args[0];
			numOfPartitions = Integer.parseInt(args[1]);
			rho = Float.parseFloat(args[2]);
			dim = Integer.parseInt(args[3]);
			minPts = Integer.parseInt(args[4]);
			epsilon = Float.parseFloat(args[5]);
		}
		
		if(args.length > 6)
			 metaBlockWindow = Integer.parseInt(args[6]);
		
		if(args.length > 7)
			if(args[7].equals("true") || args[7].equals("TRUE"))
				boost = true;
	}

	
	public static SparkConf setSparkConfiguration(String numOfInstance, String numOfCore, String exeMemory, String driverMemory, String overHeap)
	{ 
		SparkConf sparkConf = new SparkConf().setAppName("RP_DBSCAN");
		sparkConf.set("spark.executor.instances", numOfInstance);
		sparkConf.set("spark.executor.cores", numOfCore);
		sparkConf.set("spark.executor.memory", exeMemory);
		sparkConf.set("spark.driver.memory", driverMemory);
		sparkConf.set("spark.driver.maxResultSize", "8g");
		sparkConf.set("spark.yarn.submit.file.replication", "2");
		sparkConf.set("spark.yarn.driver.memoryOverhead", overHeap);
		sparkConf.set("spark.yarn.executor.memoryOverhead", overHeap);
		sparkConf.set("spark.shuffle.service.enabled", "true");
		sparkConf.set("spark.shuffle.memoryFraction", "0.5");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.kryoserializer.buffer.mb", "256");
		sparkConf.set("spark.memory.fraction", "0.7");
		sparkConf.registerKryoClasses(new Class<?>[]{ArrayList.class, Edge.class, Point.class, Null.class, Cell.class, ApproximatedCell.class, ApproximatedPoint.class, LabeledCell.class, HashMap.class});	
		return sparkConf;
	}
	
}

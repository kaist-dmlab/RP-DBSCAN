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
	public static String outputPath = null;
	public static int numOfPartitions = 0;
	public static float rho = 0;
	public static int dim = 0;
	public static int minPts = 0;
	public static float epsilon = 0;
	public static int metaBlockWindow = 1;
	public static boolean boost = false;
	
	//additional parameters
	public static String delimeter = " ";
	public static String metaFoler = "DICTIONARY";
	public static String convertTableFolder = "CONVERTS";
	public static String coreInfoFolder = "CORE_INFO";
	public static String metaResult = "GLOBAL_CELL_GRAPH";
	public static int limitDimForVirtualCombining = 6;
	public static int numOflvhCellsInMetaPartition = 12000000;
	public static int limitNumOflv1Cell = 1000000;
	
	public static void setInputParameters(String[] args)
	{	
		parseInputForm(args);
		
		if(inputPath == null || numOfPartitions <= 1 || rho == 0 || dim == 0 || minPts == 0 || epsilon == 0)
		{
			System.out.println("Usage: <inputPath> <numOfPartitions> <rho> <dim> <minPtr> <epsilon> <optional : metaBlockWindow> <optional : outputPath>");
			System.out.println("-i : the path for input dataset.");
			System.out.println("-np : the number of cores or partitions which you want to set.");
			System.out.println("-rho : approximation rate.");
			System.out.println("-dim : dimension of dataset.");
			System.out.println("-minPts : minumum number of points in neighborhood to be core point.");
			System.out.println("-eps : region query boundary.");
			System.out.println("-bs : block size for virtually combining two-level cell dictionary. (default : 1)");
			System.out.println("-o : the path to write the clustering result. the result is written as <pointId, label> pairs.");
			System.exit(1);
		}
	}

	public static void parseInputForm(String[] args)
	{
		if(args.length % 2 != 0)
			System.out.println("Argument parsing error.");
		
		String header = "";
		String value = "";
		for(int i=0; i<args.length; i+=2)
		{
			header = args[i];
			value = args[i+1];
			
			if(header.equals("-i"))
				inputPath = value;
			else if(header.equals("-o"))
				outputPath = value;
			else if(header.equals("-np"))
				numOfPartitions = Integer.parseInt(value);
			else if(header.equals("-rho"))
				rho = Float.parseFloat(value);
			else if(header.equals("-dim"))
				dim = Integer.parseInt(value);
			else if(header.equals("-minPts"))
				minPts = Integer.parseInt(value);
			else if(header.equals("-eps"))
				epsilon = Float.parseFloat(value);
			else if(header.equals("-bs"))
				 metaBlockWindow = Integer.parseInt(value);
			//We are now testing this code to boost our algorithm.
			else if(header.equals("-boost"))
			{	
				if(header.equals("true") || value.equals("TRUE"))
					boost = true;
			}
			else
			{
				System.out.println("Argument parsing error!");
				System.exit(1);
			}
		}

		System.out.println("-i : " + inputPath);
		System.out.println("-np : " + numOfPartitions);
		System.out.println("-rho : " + rho);
		System.out.println("-dim : " + dim);
		System.out.println("-minPts : " + minPts);
		System.out.println("-eps : " + epsilon);
		System.out.println("-bs : " + metaBlockWindow);
		if(outputPath != null)
			System.out.println("-o : " + outputPath);
		
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

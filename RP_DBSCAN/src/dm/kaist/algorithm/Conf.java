package dm.kaist.algorithm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.spark.SparkConf;

import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.dictionary.Cell;
import dm.kaist.graph.Edge;
import dm.kaist.graph.LabeledCell;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;

public final class Conf  implements Serializable{
	
	//input parameters
	public static String inputPath = null;
	public static String metaOutputPath = null;
	public static String pairOutputPath = null;
	public static int numOfPartitions = 0;
	public static float rho = 0;
	public static int dim = 0;
	public static int minPts = 0;
	public static float epsilon = 0;
	public static int metaBlockWindow = 1;
	public static boolean boost = false;
	
	//parameters
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
		
		if(inputPath == null || metaOutputPath == null || numOfPartitions <= 1 || rho == 0 || dim == 0 || minPts == 0 || epsilon == 0)
		{
			System.out.println("Your command must include the necessary parameters properly.");
			System.out.println("1. Necessary parameters");
			System.out.println(" -i : the hdfs path for input dataset.");
			System.out.println(" -o : the local path to write the meta result of clustering (e.g., # of (sub-)cells, # of points for each cluster).");
			System.out.println(" -np : the number of cores or partitions which you want to set.");
			System.out.println(" -rho : the approximation parameter.");
			System.out.println(" -dim : the number of dimensions.");
			System.out.println(" -minPts : the minimum number of neighbor points.");
			System.out.println(" -eps : the radius of a neighborhood.");
			System.out.println("2. Optional parameters");
			System.out.println(" -bs : the block size for virtually combining two-level cell dictionary (default : 1).");
			System.out.println(" -l : the hdfs path to write labeled points, <point id, cluster label> (default : no output).");
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
				metaOutputPath = value;
			else if(header.equals("-l"))
				pairOutputPath = value;
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

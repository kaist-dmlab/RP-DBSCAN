package dm.kaist.algorithm;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.dictionary.Cell;
import dm.kaist.dictionary.Dictionary;
import dm.kaist.dictionary.NeighborCell;
import dm.kaist.graph.Cluster;
import dm.kaist.graph.Edge;
import dm.kaist.graph.LabeledCell;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.FileIO;
import dm.kaist.io.Point;
import dm.kaist.mst.MinimumSpanningTree;
import dm.kaist.norm.Norm;
import dm.kaist.partition.Partition;
import dm.kaist.partition.Partition.SplitPosition;
import dm.kaist.tree.Kdnode;
import dm.kaist.tree.Kdtree;
import scala.Tuple2;

public class Methods implements Serializable {
	
	//Assign each point to an appropriate cell
	public static class PointToCell implements PairFunction<String, List<Integer>, Point> {
		private int dim = 0;
		private float level1SideLen = 0;
		
		public PointToCell(int dim, float epsilon) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
		    this.level1SideLen = epsilon / (float)Math.sqrt(dim);
		}
		
		@Override
		public Tuple2<List<Integer> , Point> call(String value) throws Exception {
			// TODO Auto-generated method stub
			Point pt = new Point(value, dim);
			List<Integer> key = pt.getLevel_1_Coords(level1SideLen, dim);	
			return new Tuple2(key,pt);
		}
	}
	
	public static class CreateLocalApproximatedPoint implements Function<Point, HashMap<ApproximatedPoint,Integer>>
	{
		private int dim = 0;
		private float levelhSideLen = 0;
		
		public CreateLocalApproximatedPoint(int dim, float epsilon, float p) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
			int LOWEST_LEVEL = (int)(Math.ceil(1 - Math.log10(p)/Math.log10(2)));
		    this.levelhSideLen = epsilon /  ((float)Math.sqrt(dim) * (1 << LOWEST_LEVEL-1));
		}
		
		@Override
		public HashMap<ApproximatedPoint,Integer> call(Point pt) throws Exception {
			// TODO Auto-generated method stub
			HashMap<ApproximatedPoint,Integer> emit = new HashMap<ApproximatedPoint,Integer>();
			List<Integer> levelhCoords = pt.getLevel_1_Coords(levelhSideLen, dim);
			float[] coords = new float[levelhCoords.size()];
			for(int i=0; i<coords.length; i++)
				coords[i] = (levelhCoords.get(i)+0.5f)*levelhSideLen;
			ApproximatedPoint appPt = new ApproximatedPoint(pt.id, coords);
			appPt.count = 1;
			emit.put(appPt, appPt.count);
			return emit;
		}
	}
	
	public static class LocalApproximation implements Function2<HashMap<ApproximatedPoint,Integer>, Point, HashMap<ApproximatedPoint,Integer>>
	{
		private int dim = 0;
		private float levelhSideLen = 0;
		
		public LocalApproximation(int dim, float epsilon, float p) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
			int LOWEST_LEVEL = (int)(Math.ceil(1 - Math.log10(p)/Math.log10(2)));
		    this.levelhSideLen = epsilon /  ((float)Math.sqrt(dim) * (1 << LOWEST_LEVEL-1));
		}
		
		@Override
		public HashMap<ApproximatedPoint,Integer> call(HashMap<ApproximatedPoint,Integer> map, Point pt) throws Exception {
			// TODO Auto-generated method stub		
			HashMap<ApproximatedPoint,Integer> emit = map;
			List<Integer> levelhCoords = pt.getLevel_1_Coords(levelhSideLen, dim);
			float[] coords = new float[levelhCoords.size()];
			for(int i=0; i<coords.length; i++)
				coords[i] = (levelhCoords.get(i)+0.5f)*levelhSideLen;
			ApproximatedPoint appPt = new ApproximatedPoint(pt.id, coords);
			appPt.count = 1;
			if(emit.containsKey(appPt))
				emit.put(appPt, emit.get(appPt)+appPt.count);
			else
				emit.put(appPt, appPt.count);
			return emit;
		}
	}
	
	public static class GlobalApproximation implements Function2<HashMap<ApproximatedPoint,Integer>, HashMap<ApproximatedPoint,Integer>, HashMap<ApproximatedPoint,Integer>>
	{
		private int dim = 0;
		
		public GlobalApproximation(int dim) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
		}
		
		@Override
		public HashMap<ApproximatedPoint,Integer> call(HashMap<ApproximatedPoint,Integer> l, HashMap<ApproximatedPoint,Integer> r) throws Exception {
			// TODO Auto-generated method stub		
			HashMap<ApproximatedPoint,Integer> left = l;
			HashMap<ApproximatedPoint,Integer> right = r;
			List<ApproximatedPoint> pts = new ArrayList<ApproximatedPoint>();
			for(Entry<ApproximatedPoint,Integer> entry : left.entrySet())
			{
				entry.getKey().count = entry.getValue();
				pts.add(entry.getKey());
			}
			int remainNumOfPts = 0;
			
			for(Entry<ApproximatedPoint,Integer> entry : left.entrySet())
			{
				remainNumOfPts += entry.getValue();
			}

			//point reduction by removing central objects, not boundary
			if(pts.size() > Conf.limitNumOflv1Cell)
			{
				ApproximatedPoint update = left.entrySet().iterator().next().getKey();
				left.put(update, left.get(update)+ remainNumOfPts);
				return left;
			}else{
				for(Entry<ApproximatedPoint,Integer> entry : right.entrySet())
				{
					if(left.containsKey(entry.getKey()))
						left.put(entry.getKey(), left.get(entry.getKey())+entry.getValue());
					else
						left.put(entry.getKey(), entry.getValue());
				}
			}
			return left;
		}
	}
		
	//Pseudo Random Partitioning and Preparation for virtually combining the two-level cell dictionary
	public static class PseudoRandomPartition implements PairFunction<Tuple2<List<Integer>,Iterable<Point>>, List<Integer>, ApproximatedCell>
	{
		private int dim = 0;
		private float levelpSideLen = 0;
		private int metaBlockWindow = 0;
		
		public PseudoRandomPartition(int dim, float epsilon, float p, int metaBlockWindow) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
		    int LOWEST_LEVEL = (int)(Math.ceil(1 - Math.log10(p)/Math.log10(2)));
		    this.levelpSideLen = epsilon / ((float)Math.sqrt(dim) * (1 << LOWEST_LEVEL-1));
		    this.metaBlockWindow = metaBlockWindow;
		}
		
		@Override
		public Tuple2<List<Integer>, ApproximatedCell> call(Tuple2<List<Integer>, Iterable<Point>> pts) throws Exception {
			// TODO Auto-generated method stub

			//Assign each point to a proper sub-cell
			//approximated point means the lvH sub-cell
			HashMap<List<Integer>, ApproximatedPoint> map = new HashMap<List<Integer>, ApproximatedPoint>();
			
			for(Point pt : pts._2)
			{
				List<Integer> lvH = pt.getLevel_1_Coords(levelpSideLen, dim);
				if(!map.containsKey(lvH))
				{
					ApproximatedPoint apprPt = new ApproximatedPoint(pt.id, pt.coords);
					map.put(lvH, apprPt);
					apprPt.ptsIds = new ArrayList<Long>();
				}
				map.get(lvH).count++;
				map.get(lvH).ptsIds.add(pt.id);
			}

			ApproximatedCell cell = new ApproximatedCell(pts._1);
			for(Entry<List<Integer>, ApproximatedPoint> pt : map.entrySet())
				cell.addPoint(pt.getValue());
		
			List<Integer> metaBlockId = new ArrayList<Integer>();
			
			//For Virtually Combining
			int dimOfCoord = 0;
			for(Integer i : cell.cellCoords)
			{	
				if(dimOfCoord >= Conf.limitDimForVirtualCombining)
					break;
				
				//Block id for virtually combining
				metaBlockId.add(i/metaBlockWindow);
				dimOfCoord ++;
			}
			return new Tuple2<List<Integer>, ApproximatedCell>(metaBlockId, cell);
		}
	}
	
	//Version 2, Pseudo Random Partitioning and Preparation for virtually combining the two-level cell dictionary
	public static class PseudoRandomPartition2 implements PairFunction<Tuple2<List<Integer>,HashMap<ApproximatedPoint,Integer>>, List<Integer>, ApproximatedCell>
	{
		private int metaBlockWindow = 0;
		
		public PseudoRandomPartition2(int metaBlockWindow) {
			// TODO Auto-generated constructor stub
		    this.metaBlockWindow = metaBlockWindow;
		}
		
		@Override
		public Tuple2<List<Integer>, ApproximatedCell> call(Tuple2<List<Integer>, HashMap<ApproximatedPoint,Integer>> approximatedMap) throws Exception {
			// TODO Auto-generated method stub
			ApproximatedCell cell = new ApproximatedCell(approximatedMap._1);
			for(Entry<ApproximatedPoint, Integer> entry : approximatedMap._2.entrySet())
			{
				entry.getKey().count = entry.getValue();
				cell.addPoint(entry.getKey());
			}
			List<Integer> metaBlockId = new ArrayList<Integer>();
			int dimOfCoord = 0;
			for(Integer i : cell.cellCoords)
			{	
				if(dimOfCoord >= Conf.limitDimForVirtualCombining)
					break;
				
				metaBlockId.add(i/metaBlockWindow);
				dimOfCoord ++;
			}
			return new Tuple2<List<Integer>, ApproximatedCell>(metaBlockId, cell);
		}
	}
	
	
	public static class Repartition implements PairFunction<Tuple2<List<Integer>,ApproximatedCell>, Integer, ApproximatedCell>
	{

		@Override
		public Tuple2<Integer, ApproximatedCell> call(Tuple2<List<Integer>, ApproximatedCell> arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Integer, ApproximatedCell>((int)(Math.random()*Conf.numOfPartitions), arg0._2);
		}
	}

	public static class CountCorePts implements PairFunction<Tuple2<Long,ApproximatedCell>, Integer, Long>
	{
		@Override
		public Tuple2<Integer, Long> call(Tuple2<Long, ApproximatedCell> arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Integer, Long>(1, (long)arg0._2.getRealPtsCount());
		}
	}
	
	
	//Virtually combining sub-dictionaries
	public static class MetaBlockMergeWithApproximation implements PairFunction<Tuple2<List<Integer>,ApproximatedCell>, List<Integer>, Long>
	{
		@Override
		public Tuple2<List<Integer>, Long> call(Tuple2<List<Integer>, ApproximatedCell> block) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<List<Integer>, Long>(block._1, (long)block._2.getApproximatedPtsCount());
		}
	}
	
	//Assign each points to build sub-dictionaries
	public static class AssignApproximatedPointToPartition implements PairFlatMapFunction<Tuple2<List<Integer>, ApproximatedCell>, Integer, ApproximatedCell> {

		HashMap<List<Integer>,List<Integer>> partitionIndex;
		public AssignApproximatedPointToPartition(HashMap<List<Integer>,List<Integer>> partitionIndex) {
			// TODO Auto-generated constructor stub	
			this.partitionIndex = partitionIndex;
		}
		
		@Override
		public Iterator<Tuple2<Integer, ApproximatedCell>> call(Tuple2<List<Integer>, ApproximatedCell> cell) throws Exception {
			// TODO Auto-generated method stub
			
			List<Tuple2<Integer, ApproximatedCell>> emits = new ArrayList<Tuple2<Integer, ApproximatedCell>>();
						List<Integer> partitionIds = partitionIndex.get(cell._1);
			
			if(partitionIds.size() == 1)
				emits.add(new Tuple2<Integer, ApproximatedCell>(partitionIds.get(0), cell._2));
			else
			{
				ApproximatedCell cells[] = new ApproximatedCell[partitionIds.size()];
				for(int i = 0; i< partitionIds.size(); i++)
					cells[i] = new ApproximatedCell(cell._2.cellCoords);
				
				for(ApproximatedPoint pt : cell._2.pts)
					cells[(int)(Math.random()*partitionIds.size())].addPoint(pt);
				
				for(int i = 0; i< partitionIds.size(); i++)
				{
					cells[i].cellId = cell._2.cellId;
					emits.add(new Tuple2<Integer, ApproximatedCell>(partitionIds.get(i), cells[i]));
				}
			}
			return emits.iterator();
		}
	}
	
	//Build sub-dictionaries
	public static class MetaGenerationWithApproximation implements PairFunction<Tuple2<Integer , Iterable<ApproximatedCell>>, Null, Null>
	{
		private int dim = 0;
		private float epsilon = 0;
		private float p = 0;
		private int minPtr = 0;
		private Configuration conf = null;
		private Dictionary meta = null;
		private List<Partition> wholePartitions = null;

		public MetaGenerationWithApproximation(int dim, float epsilon, float p, int minPtr, Configuration conf, List<Partition> wholePartitions) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
			this.epsilon = epsilon;
			this.p = p;
			this.minPtr = minPtr;
			this.conf = conf;
			this.wholePartitions = wholePartitions;
		}

		@Override
		public Tuple2<Null,Null> call(Tuple2<Integer , Iterable<ApproximatedCell>> partition)
				throws Exception {
			// TODO Auto-generated method stub
	
			meta = new Dictionary(dim, epsilon, p, minPtr);
			meta.generateMetaDataWithApproximation(partition._2);
			long metaFileName = (long)System.currentTimeMillis();
			
			//System.out.println("Size of Metadata : "+ (meta.level_1_Meta.length*4 + meta.level_2_Meta.length + meta.level_2_Count.length* 4) + " bytes");
	
			//serialization + gzip compression
			FileSystem fs = FileSystem.get(conf);
			BufferedOutputStream bw = new BufferedOutputStream(fs.create(new Path(Conf.metaFoler+"/"+metaFileName+"_"+(int)(Math.random()*10000)+"_"+(int)(Math.random()*10000))));
			GZIPOutputStream gz = new GZIPOutputStream(bw);
			ObjectOutputStream obs = new ObjectOutputStream(gz);
			obs.writeObject(meta);
			obs.close();
			gz.close();
			bw.close();
			
			return new Tuple2<Null, Null>(null, null);	
		}
	}
	
	//Core marking procedure
	public static class FindCorePointsWithApproximation implements PairFlatMapFunction<Iterator<Tuple2<Integer,ApproximatedCell>>, Long, ApproximatedCell>
	{
		private int dim = 0;
		private int minPts = 0;
		private float eps = 0;
		private float sqr_r = 0;
		private Configuration conf = null;
		private List<String> metaPaths = null;
		
		public FindCorePointsWithApproximation(int dim, float epsilon, int minPts, Configuration conf, List<String> metaPaths){
			this.minPts = minPts;
			this.eps = epsilon;
			this.conf = conf;
			this.metaPaths = metaPaths;
			this.sqr_r = epsilon*epsilon;
			this.dim = dim;
		}
		
		@Override
		public Iterator<Tuple2<Long, ApproximatedCell>> call(Iterator<Tuple2<Integer, ApproximatedCell>> args)
				throws Exception {
			// TODO Auto-generated method stub
			
			List<ApproximatedCell> grids = new ArrayList<ApproximatedCell>();
			while(args.hasNext())
			{
				Tuple2<Integer, ApproximatedCell> temp = args.next();

				if(temp._2.getRealPtsCount() < minPts)
				{
					for(ApproximatedPoint pt : temp._2.pts)
						pt.neighborPts = temp._2.getRealPtsCount();
				}else
				{
					temp._2.ifFullCore = true;
					for(ApproximatedPoint pt : temp._2.pts)
						pt.isCore = true;
				}
				
				grids.add(temp._2);
			}

			//load meta directory info
			int metaSize = metaPaths.size();
	
			//set read order randomly
			HashSet<Integer> readOrders = new HashSet<Integer>();
			while(readOrders.size() < metaSize)
				readOrders.add((int)(Math.random()*metaSize));
			
			int id = 1;
			for(Integer readOrder : readOrders)
			{
				System.out.println("Meta Id : " + (id++));
				findCoreWithSpecificMeta(readOrder, grids);
			}

			List<Tuple2<Long, ApproximatedCell>> emits = new ArrayList<Tuple2<Long, ApproximatedCell>>();
			HashSet<Long> coreCellIds = new HashSet<Long>();
			for(ApproximatedCell grid : grids)
			{
				List<ApproximatedPoint> corePts = new ArrayList<ApproximatedPoint>();
				boolean isCoreCell = false;
				
				for(ApproximatedPoint pt : grid.pts)
				{
					if(pt.isCore)
					{
						corePts.add(pt);
						isCoreCell = true;
					}
				}
			
				if(isCoreCell)
				{
					coreCellIds.add(grid.cellId);
					grid.pts = corePts;
					emits.add(new Tuple2<Long, ApproximatedCell>(grid.cellId, grid));
				}
			}
			
			//write core cell id in hdfs
			FileSystem fs = FileSystem.get(conf);
			BufferedOutputStream bw = new BufferedOutputStream(fs.create(new Path(Conf.coreInfoFolder+"/"+System.currentTimeMillis())));
			GZIPOutputStream gz = new GZIPOutputStream(bw);
			ObjectOutputStream obs = new ObjectOutputStream(gz);
			obs.writeObject(coreCellIds);
			obs.close();
			gz.close();
			bw.close();

			return emits.iterator();
		}
		
		public void findCoreWithSpecificMeta(int readOrder, List<ApproximatedCell> grids) throws IOException, ClassNotFoundException
		{
			float[] coords = new float[dim];
			List<Integer> neighborIdList = new ArrayList<Integer>();
			
			List<Integer> key = null;
			List<ApproximatedPoint> innerPts = null;
			int comp = (int)(Math.ceil(Math.sqrt(dim)));
				
			BufferedInputStream bi = new BufferedInputStream(new FileInputStream(new File(SparkFiles.get(metaPaths.get(readOrder)))));	
			GZIPInputStream gis = new GZIPInputStream(bi);
			ObjectInputStream ois = new ObjectInputStream(gis);
			Dictionary meta = (Dictionary)ois.readObject();
			ois.close();
			gis.close();
			bi.close();
				
			meta.buildNeighborSearchTree();
				
			for(ApproximatedCell grid : grids)
			{
				key = grid.cellCoords;
				innerPts = grid.pts;
					
				if( grid.ifFullCore || !meta.isContainCell(key))
					continue;

				//find neighbor cell from i th partition
				for(int j=0; j<dim; j++)
					coords[j] = (float)key.get(j);
							
				neighborIdList.clear();
				meta.neighborTree.getNeighborId(meta.neighborTree.root, coords, neighborIdList, comp);
					
				int state = 0;
				int cnt = 0;
					
				for(int j=0; j<neighborIdList.size(); j++)
				{						
					List<Integer> neighborCoords = meta.getIntGirdCoordsIndex(neighborIdList.get(j));
					Kdtree kdtree = meta.lvp_neighborTrees.get(neighborIdList.get(j));
					NeighborCell neighbor = new NeighborCell(neighborCoords, kdtree);

					for(ApproximatedPoint pt : innerPts)
					{
						if(pt.isCore)	continue;
						cnt = pt.neighborPts;

						//state check
						state = pt.stateWithSphere(neighbor.cellId, dim, sqr_r, meta.level_1_SideLen);
						if(state == 1)
							cnt += neighbor.lv_p_kdtree.count;
						else if(state == 0)
						{
							List<Kdnode> lv_p_neighbor = new ArrayList<Kdnode>();
							neighbor.lv_p_kdtree.getNeighborNode(neighbor.lv_p_kdtree.root, pt.coords, lv_p_neighbor, eps);
							for(Kdnode node: lv_p_neighbor)
							{
								if(Norm.sqr_L2_norm(pt.coords, node.coords) <= sqr_r)
									cnt += node.count;
								if(cnt >= minPts)
									break;
							}
						}
							
						pt.neighborPts = cnt;	
							
						if(cnt >= minPts)
							pt.isCore = true;
					}
				}
			}
			meta = null;
		}
	}
	
	//Cell graph contruction
	public static class FindDirectDensityReachableEdgesWithApproximation implements PairFlatMapFunction<Iterator<Tuple2<Long,ApproximatedCell>>, Integer, Edge>
	{
		//for neighbor search
		private Dictionary meta = null;
		private Configuration conf = null;
		private List<String> metaPaths = null;
		private List<String> corePaths = null;
		
		//for neighbor cell
		private int minPts;
		private int dim;
		private float sqr_r;
		private float epsilon;
		private Kdtree coreTree = null;
		private int numOfPartition = 0;
			
		public FindDirectDensityReachableEdgesWithApproximation(int dim, float epsilon, int minPts, Configuration conf, List<String> metaPaths, List<String> corePaths,int numOfPartition) {
			// TODO Auto-generated constructor stub
			this.conf = conf;	
			this.dim = dim;
			this.minPts = minPts;
			this.metaPaths = metaPaths;
			this.corePaths = corePaths;
			//for direct density reachable search
			this.epsilon = epsilon;
			this.sqr_r = epsilon*epsilon;
			this.coreTree = new Kdtree(dim);
			this.numOfPartition = numOfPartition;
		}
		
		@Override
		public Iterator<Tuple2<Integer, Edge>> call(Iterator<Tuple2<Long,ApproximatedCell>> args)
				throws Exception {
			// TODO Auto-generated method stub
		
			HashSet<Edge> edges = new HashSet<Edge>();
			List<ApproximatedCell> grids = new ArrayList<ApproximatedCell>();
			
			while(args.hasNext())
				grids.add(args.next()._2);
			
			//load meta directory info
			int metaSize = metaPaths.size();

			//set read order randomly
			HashSet<Integer> readOrders = new HashSet<Integer>();
			while(readOrders.size() < metaSize)
				readOrders.add((int)(Math.random()*metaSize));
			
			int ids = 1;
			for(Integer readOrder : readOrders)
			{
				System.out.println("DDR Meta ID : "+ (ids++));
				findDDRWithSpecificMeta(readOrder, edges, grids);
			}
			
			//------------------Edge Reduction 1 iteration
			HashSet<Long> mergedCoreCells = new HashSet<Long>();
			metaSize = corePaths.size();
			for(int i=0; i<metaSize; i++)
			{
				BufferedInputStream bi = new BufferedInputStream(new FileInputStream(new File(SparkFiles.get(corePaths.get(i)))));
				GZIPInputStream gis = new GZIPInputStream(bi);
				ObjectInputStream ois = new ObjectInputStream(gis);
				HashSet<Long> temp = (HashSet<Long>)ois.readObject();
				mergedCoreCells.addAll(temp);
				ois.close();
				gis.close();
				bi.close();
			}
			
			MinimumSpanningTree tree = new MinimumSpanningTree();
			return tree.reduceEdgesByMST(mergedCoreCells, edges, numOfPartition/2).iterator();
		}
		
		public void findDDRWithSpecificMeta(int readOrder, HashSet<Edge> edges, List<ApproximatedCell> grids) throws IOException, ClassNotFoundException
		{
				BufferedInputStream bi = new BufferedInputStream(new FileInputStream(new File(SparkFiles.get(metaPaths.get(readOrder)))));	
				GZIPInputStream gis = new GZIPInputStream(bi);
				ObjectInputStream ois = new ObjectInputStream(gis);
				Dictionary meta = (Dictionary)ois.readObject();
				ois.close();
				gis.close();
				bi.close();
				meta.buildNeighborSearchTree();
				
				float[] coords = new float[dim];
				List<Integer> neighborIdList = new ArrayList<Integer>();

				List<Integer> targetId = null;
				Iterable<ApproximatedPoint> corePts = null;

				int comp = (int)(Math.ceil(Math.sqrt(dim)));
				
				for(ApproximatedCell grid : grids)
				{
					targetId = grid.cellCoords;
					corePts = grid.pts;
					
					if(!meta.isContainCell(targetId))
						continue;
					
					//tree for fast search
					coreTree.clear();
					int index = 0;
					for(ApproximatedPoint corePt : corePts)
						coreTree.insert(index, corePt.coords, 1);
					
					//find neighbor cell from i th partition
					for(int j=0; j<dim; j++)
						coords[j] = (float)targetId.get(j);
					
					neighborIdList.clear();
					meta.neighborTree.getNeighborId(meta.neighborTree.root, coords, neighborIdList, comp);

					for(int j=0; j<neighborIdList.size(); j++)
					{
						long neighborEncodedId = meta.getLv1CellEncodedId(neighborIdList.get(j));
						Edge edge = new Edge(grid.cellId, neighborEncodedId, ((float)Math.random()));							
						
						if(edges.contains(edge))
							continue;
						
						List<Integer> neighborId = meta.getIntGirdCoordsIndex(neighborIdList.get(j));
						Kdtree kdtree = meta.lvp_neighborTrees.get(neighborIdList.get(j));
						NeighborCell neighbor = new NeighborCell(neighborId, kdtree);
									
						List<Integer> edgeKey = new ArrayList<Integer>();
						edgeKey.addAll(targetId); edgeKey.addAll(neighborId);
												
						List<Integer> inverseKey = new ArrayList<Integer>();
						inverseKey.addAll(neighborId); inverseKey.addAll(targetId);
												
						Kdnode node = coreTree.cloestNode(neighbor.lv_p_kdtree.root.coords, sqr_r);
						Kdnode closest = null;
						if(node != null)
							closest = neighbor.lv_p_kdtree.cloestNode(node.coords,sqr_r);
						
						if(node == null || closest == null || Norm.sqr_L2_norm(node.coords, closest.coords) <= sqr_r)
							edges.add(edge);
					}
				}
				meta = null;
			}
	}	

	public static List<Partition> scalablePartition(List<Tuple2<List<Integer>, Long>> cellData, int dim, int maxNumOfSubcells, HashMap<List<Integer>, List<Integer>> cellIdToPartitionId)
	{
		//As the number of dimensions increases, the number of meta blocks can be extremely increases. 
		//Therefore, we limit the number of dimensions to apply BSP approach for dividing our two-level cell dictionary into multiple contiguous sub-dictionaries.
		if(dim > Conf.limitDimForVirtualCombining)
			dim = Conf.limitDimForVirtualCombining;
		
		Partition initPartition = new Partition(cellData, dim);
		
		List<Partition> wholePartitions = new ArrayList<Partition>();
		wholePartitions.add(initPartition);
		List<Partition> removedPartitions = new ArrayList<Partition>();
		List<Partition> addedPartitions = new ArrayList<Partition>();
		
		while(true)
		{
			boolean endCondition = true;
			removedPartitions.clear();
			addedPartitions.clear();
			
			for(int i=0; i<wholePartitions.size(); i++)
			{

				Partition partition = wholePartitions.get(i);
				if(partition.getPtsCount() >= maxNumOfSubcells && partition.isLargerThanMinSize())
				{
					//find best axis for dividing
					List<Partition> subPartitions = partition.findBestSplitedPartition();

					addedPartitions.addAll(subPartitions);
					removedPartitions.add(partition);
					endCondition = false;
				}
			}

			if(!endCondition)
			{
				wholePartitions.removeAll(removedPartitions);
				wholePartitions.addAll(addedPartitions);
			}else
				break;
		}
		
		//Assign partition id
		int partitionId = 0;
		for(Partition partition : wholePartitions)
		{
			partition.setPartitionId(partitionId++);
		}
		
		//If the size of value is not 1, then it is overlap Cell
	
		for(Partition partition : wholePartitions)
		{
			for(Tuple2<List<Integer>, Long> cell : partition.subCells)
			{
				List<Integer> cellId = cell._1;
				if(!cellIdToPartitionId.containsKey(cellId))
					cellIdToPartitionId.put(cellId, new ArrayList<Integer>());

				cellIdToPartitionId.get(cellId).add(partition.partitionId);
			}
		}

		//exception expansion
		removedPartitions.clear();
		addedPartitions .clear();
		
		for(Partition partition : wholePartitions)
		{
			if(partition.getPtsCount() > maxNumOfSubcells)
			{
				int numOfOverlapPartition = (int)(Math.ceil(partition.getPtsCount()/(float)maxNumOfSubcells));
				
				for(Tuple2<List<Integer>, Long> cell : partition.subCells)
				{
					List<Integer> cellId = cell._1;
					cellIdToPartitionId.get(cellId).clear();			
					for(int i=0; i<numOfOverlapPartition; i++)
						cellIdToPartitionId.get(cellId).add(partitionId+i);		
				}
				
				for(int i=0; i<numOfOverlapPartition; i++)
				{
					Partition p = new Partition(partitionId+i);
					p.count = (int)(partition.getPtsCount()/(float)numOfOverlapPartition);
					addedPartitions.add(p);
				}

				removedPartitions.add(partition);
				partitionId += numOfOverlapPartition;
			}
		}
		wholePartitions.removeAll(removedPartitions);
		wholePartitions.addAll(addedPartitions);
		
		return wholePartitions;
	}
	
	//Cell sub-graph merger & Edge detection & Edge reduction
	public static class BuildMST implements PairFlatMapFunction<Iterator<Tuple2<Integer,Edge>>, Integer, Edge>
	{
		private Configuration conf = null;
		private int nextPartionSize = 0;
		private List<String> corePaths = null;
		private int tastNum = 1;
		
		public BuildMST(Configuration conf, List<String> corePaths, int nextPartitonSize)
		{
			this.conf = conf;
			this.corePaths = corePaths;
			this.nextPartionSize = nextPartitonSize;
			
		}
		
		@Override
		public Iterator<Tuple2<Integer, Edge>> call(Iterator<Tuple2<Integer, Edge>> args) throws Exception {
			// TODO Auto-generated method stub
			HashSet<Long> mergedCoreCells = new HashSet<Long>();
			
			//load core cell ids
			int metaSize = corePaths.size();
			for(int i=0; i<metaSize; i++)
			{
				BufferedInputStream bi = new BufferedInputStream(new FileInputStream(new File(SparkFiles.get(corePaths.get(i)))));
				GZIPInputStream gis = new GZIPInputStream(bi);
				ObjectInputStream ois = new ObjectInputStream(gis);
				HashSet<Long> temp = (HashSet<Long>)ois.readObject();
				mergedCoreCells.addAll(temp);
				ois.close();
				gis.close();
				bi.close();
			}

			List<Edge> edges = new ArrayList<Edge>();
			while(args.hasNext())
				edges.add(args.next()._2);
			
			for(Edge edge : edges)
			{
				if( mergedCoreCells.contains(edge.u) && mergedCoreCells.contains(edge.v))
						edge.setCore(true);
			}
			
			HashMap<Long, List<Edge>> coreEdges = new HashMap<Long, List<Edge>>();
			List<Edge> unknownEdges = new ArrayList<Edge>();
		
			//divide edges to core edges and unknown edges
			for(Edge edge : edges)
			{
				if(edge.isCore)
				{
					if(!coreEdges.containsKey(edge.u))
						coreEdges.put(edge.u, new ArrayList<Edge>());	
					coreEdges.get(edge.u).add(edge);
					
					if(!coreEdges.containsKey(edge.v))
						coreEdges.put(edge.v, new ArrayList<Edge>());	
					
					Edge inverseEdge = new Edge(edge.v, edge.u, edge.weight);
					inverseEdge.setCore(true);
					coreEdges.get(edge.v).add(inverseEdge);
					
				}else
					unknownEdges.add(edge);
			}
			
			long id = (int)System.currentTimeMillis();
			FileSystem fs = FileSystem.get(conf);
			BufferedOutputStream result_output = new BufferedOutputStream(fs.create(new Path("EDGES/"+id)));
			GZIPOutputStream gz = new GZIPOutputStream(result_output);
			ObjectOutputStream obs = new ObjectOutputStream(gz);
			obs.writeObject(coreEdges);
			obs.close();
			gz.close();

			result_output.close();
	
			List<Cluster> clusters = new ArrayList<Cluster>();
		
			MinimumSpanningTree tree = new MinimumSpanningTree();
			List<Edge> updatedEdge = tree.BuildMinimumSpanningForest(coreEdges, clusters);

			updatedEdge.addAll(unknownEdges);

			List<Tuple2<Integer, Edge>> emits = new ArrayList<Tuple2<Integer,Edge>>();
			for(Edge edge : updatedEdge)
				emits.add(new Tuple2<Integer, Edge>((int)(Math.random()*nextPartionSize), edge));
			
			tastNum++;
			return emits.iterator();
		}
		
	}
	
	//Build global cell graph
	public static class FinalPhase implements PairFlatMapFunction<Iterator<Tuple2<Integer,Edge>>, Integer, Integer>
	{
		private Configuration conf = null;
		private HashSet<Long> mergedCoreCells = null;
		private List<String> corePaths = null;
		
		public FinalPhase(Configuration conf, List<String> corePaths)
		{
			this.conf = conf;
			this.mergedCoreCells = new HashSet<Long>();
			this.corePaths = corePaths;
		
		}
		
		@Override
		public Iterator<Tuple2<Integer,Integer>> call(Iterator<Tuple2<Integer, Edge>> args) throws Exception {
			// TODO Auto-generated method stub
			
			//load core cell ids
			FileSystem fs = FileSystem.get(conf);
			int metaSize = corePaths.size();
			for(int i=0; i<metaSize; i++)
			{
				BufferedInputStream bi = new BufferedInputStream(new FileInputStream(new File(SparkFiles.get(corePaths.get(i)))));
				GZIPInputStream gis = new GZIPInputStream(bi);
				ObjectInputStream ois = new ObjectInputStream(gis);
				HashSet<Long> temp = (HashSet<Long>)ois.readObject();
				mergedCoreCells.addAll(temp);
				ois.close();
				gis.close();
				bi.close();
			}
			

			List<Edge> edges = new ArrayList<Edge>();
			List<Edge> oneCellCluster = new ArrayList<Edge>();
			while(args.hasNext())
				edges.add(args.next()._2);
			
			for(Edge edge : edges)
			{
				if( mergedCoreCells.contains(edge.u) && mergedCoreCells.contains(edge.v))
						edge.setCore(true);
			}
			
			HashMap<Long, List<Edge>> coreEdges = new HashMap<Long, List<Edge>>();
			List<Edge> unknownEdges = new ArrayList<Edge>();
			
			//divide edges to core edges and unknown edges
			for(Edge edge : edges)
			{
				if(edge.isCore)
				{
					if(!coreEdges.containsKey(edge.u))
						coreEdges.put(edge.u, new ArrayList<Edge>());	
					coreEdges.get(edge.u).add(edge);
					
					if(!coreEdges.containsKey(edge.v))
						coreEdges.put(edge.v, new ArrayList<Edge>());	
					
					Edge inverseEdge = new Edge(edge.v, edge.u, edge.weight);
					inverseEdge.setCore(true);
					coreEdges.get(edge.v).add(inverseEdge);
					
				}else if(edge.weight == -1f)
				{
					oneCellCluster.add(edge);
				}
				else
					unknownEdges.add(edge);
			}
			
			List<Cluster> clusters = new ArrayList<Cluster>();
			MinimumSpanningTree tree = new MinimumSpanningTree();
			List<Edge> updatedEdge = tree.BuildMinimumSpanningForest(coreEdges, clusters);

			/*
			 * Case 1 : definitely core cell, but not included in cluster 
			 * 	type 1 : core cell without any neighbor => 1 core & no neighbor cluster!
			 *  type 2 : core cell with neighbors => 1 core & neighbor/s cluster!
			 */
			HashSet<Long> borderCellIds = new HashSet<Long>();
			
			for(Long coreCellId : mergedCoreCells)
			{
				boolean assigned = false;
				
				for(int i=0; i<clusters.size(); i++)
				{
					if(clusters.get(i).coreCellIds.contains(coreCellId))
					{	
						assigned = true;
						break;
					}
				}
				
				if(!assigned)
				{
					HashSet<Long> coreCellIds = new HashSet<Long>();
					coreCellIds.add(coreCellId);
					Cluster cluster = new Cluster(clusters.size()+1,coreCellIds);
					clusters.add(cluster);	
				}
			}
			
			/*
			 * Case 2 
			 * type 1: border cell assign to 1 cluster
			 * type 2: more than 2 cluster share same boundary cell
			 *  we should process point based calculation to assign each point in border cell!
			 */
			HashMap<Long,List<Edge>> coreWithNeighbors = new HashMap<Long,List<Edge>>();
			for(Edge edge : unknownEdges)
			{
				borderCellIds.add(edge.v);
				coreWithNeighbors.put(edge.v, new ArrayList<Edge>());
			}
			
			for(Edge edge : unknownEdges)
				coreWithNeighbors.get(edge.v).add(edge);
			
			Set<Entry<Long, List<Edge>>> entries = coreWithNeighbors.entrySet();
			for(Entry<Long, List<Edge>> entry : entries)
			{
				Long borderCellId = entry.getKey();
				List<Edge> assignedCellIds = entry.getValue();
				
				for(Edge edge : assignedCellIds)
				{
					for(Cluster cluster : clusters)
					{
						//insert border cell id and the connected corecell ids to assigned cluster
						if(cluster.coreCellIds.contains(edge.u))
						{
							if(!cluster.borderCellIds.containsKey(borderCellId))
								cluster.borderCellIds.put(borderCellId, new ArrayList<Long>());
							cluster.borderCellIds.get(borderCellId).add(edge.u);
						}
					}
				}	
			}
			
			//write meta_result file
			BufferedOutputStream result_output = new BufferedOutputStream(fs.create(new Path(Conf.metaResult+"/meta_result")));
			GZIPOutputStream gz = new GZIPOutputStream(result_output);
			ObjectOutputStream obs = new ObjectOutputStream(gz);
			obs.writeObject(clusters);
			obs.close();
			gz.close();
			result_output.close();
			
			List<Tuple2<Integer, Integer>> emit = new ArrayList<Tuple2<Integer,Integer>>();
			emit.add(new Tuple2<Integer, Integer>(0, clusters.size()));

			return emit.iterator();
		}
	}

	//Find predecessor cells of non-core cells
	public static class EmitConnectedCoreCellsFromBorderCell implements PairFlatMapFunction<Tuple2<Integer, ApproximatedCell>, Long, LabeledCell>
	{
		public List<Cluster> clusters;
		public int numOfPartition;
		
		HashSet<Long> borderCells;
		HashMap<Long, List<Long>> connectedNeighbors;
		HashMap<Long, Integer> clusterIdMap;
		
		public EmitConnectedCoreCellsFromBorderCell(Configuration conf, int numOfPartition)
		{
			this.numOfPartition = numOfPartition;
			
			FileSystem fs = null;
			try {
			fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(Conf.metaResult));
			BufferedInputStream bi = new BufferedInputStream(fs.open(status[0].getPath()));
			GZIPInputStream gis = new GZIPInputStream(bi);
			ObjectInputStream ois = new ObjectInputStream(gis);
			List<Cluster> clusters = (List<Cluster>)ois.readObject();
			ois.close();
			gis.close();
			bi.close();

			borderCells = new HashSet<Long>();
			connectedNeighbors = new HashMap<Long, List<Long>>();
			clusterIdMap = new HashMap<Long, Integer>();
			
			for(Cluster cluster: clusters)
			{
				int clusterId = cluster.clusterId;
				HashMap<Long,List<Long>> borderCellIds = cluster.borderCellIds;
				Set<Entry<Long, List<Long>>> entries = borderCellIds.entrySet();
				for(Entry<Long, List<Long>> entry : entries)
				{
					borderCells.add(entry.getKey());
					for(Long coreCell : entry.getValue())
					{
						if(!connectedNeighbors.containsKey(coreCell))
							connectedNeighbors.put(coreCell, new ArrayList<Long>());
						
						if(!clusterIdMap.containsKey(coreCell))
							clusterIdMap.put(coreCell, clusterId);
						
						connectedNeighbors.get(coreCell).add(entry.getKey());
					}
				}	
			}

			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		@Override
		public Iterator<Tuple2<Long, LabeledCell>> call(Tuple2<Integer, ApproximatedCell> grid) throws Exception {
			// TODO Auto-generated method stub
			
			List<Tuple2<Long, LabeledCell>> emits = new ArrayList<Tuple2<Long, LabeledCell>>();
			
			Long cellId = grid._2.cellId;
			List<ApproximatedPoint> pts = grid._2.pts;
			
			//case 1: if cell is border cell
			
			if(borderCells.contains(cellId))
			{
				LabeledCell temp = new LabeledCell(-1, cellId, pts);
				//emit border cell data
				emits.add(new Tuple2<Long, LabeledCell>(cellId, temp));
			}
			
			//case 2: if cell is core cell connecting border
			else if(connectedNeighbors.containsKey(cellId))
			{
				int clusterId = clusterIdMap.get(cellId);
				LabeledCell temp = new LabeledCell(clusterId, cellId, pts);
				for(Long borderid : connectedNeighbors.get(cellId))
				{
					emits.add(new Tuple2<Long, LabeledCell>(borderid, temp));
				}
			}

			return emits.iterator();
		}
	}
	
	//Labeling border points
	public static class AssignBorderPointToCluster implements PairFlatMapFunction<Tuple2<Long,Iterable<LabeledCell>>, Integer, ApproximatedPoint>{

		public int dim = 0;
		public float sqr_r = 0;
		private Configuration conf = null;
		public String pairOutputPath = null;
		public AssignBorderPointToCluster(int dim, float eps, Configuration conf, String pairOutputPath) {
			// TODO Auto-generated constructor stub
			this.dim = dim;
			this.sqr_r = eps*eps;
			this.conf = conf;
			this.pairOutputPath = pairOutputPath;
		}
		
		@Override
		public Iterator<Tuple2<Integer, ApproximatedPoint>> call(Tuple2<Long, Iterable<LabeledCell>> input)
				throws Exception {
			// TODO Auto-generated method stub
			
			List<Tuple2<Integer, ApproximatedPoint>> emits = new ArrayList<Tuple2<Integer, ApproximatedPoint>>();
			
			Iterable<LabeledCell> cells = input._2;
			
			LabeledCell borderCell = null;
			List<LabeledCell> connectedCoresFromBorder = new ArrayList<LabeledCell>();
			
			for(LabeledCell cell : cells)
			{	
				if(cell.clusterId == -1)
					borderCell = cell;
				else
					connectedCoresFromBorder.add(cell);
			}
			
			//build tree
			for(LabeledCell coreCell : connectedCoresFromBorder)
				coreCell.buildKdTree(dim);
			
			//---Write Result----
			FileSystem fs = null;
			BufferedOutputStream bw = null;
			String output = "";
			if(pairOutputPath != null)
			{
				fs = FileSystem.get(conf);
				bw = new BufferedOutputStream(fs.create(new Path(pairOutputPath+"/bordercell_"+borderCell.cellId)));
			}
			//-------------------
			
			
			if(borderCell != null){
			for(ApproximatedPoint pt : borderCell.pts)
			{		
				for(LabeledCell coreCell : connectedCoresFromBorder)
				{
					Kdnode node = coreCell.tree.cloestNode(pt.coords, sqr_r);
					if(node == null || Norm.sqr_L2_norm(pt.coords, node.coords) <= sqr_r)
					{
						emits.add(new Tuple2<Integer, ApproximatedPoint>(coreCell.clusterId, pt));
						
						//wirte border points
						if(pairOutputPath != null)
						{
							for(Long id : pt.ptsIds)
							{
								output = id+Conf.delimeter+coreCell.clusterId+"\n";
								bw.write(output.getBytes());
							}
						}

						//overlap
						break;
					}
				}		
			}		

			if(pairOutputPath != null)
				bw.close();
		
			}
			return emits.iterator();
		}
	}
	
	
	//Labeling core points
	public static class AssignCorePointToCluster implements PairFlatMapFunction<Iterator<Tuple2<Integer, ApproximatedCell>>, Integer, ApproximatedPoint>{

		public List<Cluster> clusters = null;
		public Configuration conf = null;
		public String pairOutputPath = null;
		
		public AssignCorePointToCluster(Configuration conf, String pairOutputPath) {
			// TODO Auto-generated constructor stub
			this.conf = conf;
			this.pairOutputPath = pairOutputPath;
			FileSystem fs = null;
			try {
			fs = FileSystem.get(this.conf);
			FileStatus[] status = fs.listStatus(new Path(Conf.metaResult));
			BufferedInputStream bi = new BufferedInputStream(fs.open(status[0].getPath()));
			GZIPInputStream gis = new GZIPInputStream(bi);
			ObjectInputStream ois = new ObjectInputStream(gis);
			this.clusters = (List<Cluster>)ois.readObject();
			ois.close();
			gis.close();
			bi.close();

			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public Iterator<Tuple2<Integer, ApproximatedPoint>> call(Iterator<Tuple2<Integer, ApproximatedCell>> grids) throws Exception {
			// TODO Auto-generated method stub
			
			List<Tuple2<Integer, ApproximatedPoint>> emits = new ArrayList<Tuple2<Integer, ApproximatedPoint>>();
			
			//copy for iteration
			List<Tuple2<Integer, ApproximatedCell>> cells = new ArrayList<Tuple2<Integer, ApproximatedCell>>();
			while(grids.hasNext())
				cells.add(grids.next());
				
			//---Write Result----
			FileSystem fs = null;
			BufferedOutputStream bw = null;
			String output = "";

			if(pairOutputPath != null)
			{
				fs = FileSystem.get(conf);
				bw = new BufferedOutputStream(fs.create(new Path(pairOutputPath+"/corecells"+cells.get(0)._2.cellId+"~")));
			}
			
			//-------------------
			
			for(Tuple2<Integer, ApproximatedCell> cell : cells)
			{
			
				Long cellId = cell._2.cellId;
				int clusterId = -1;
				
				for(Cluster cluster : clusters)
				{
					if(cluster.coreCellIds.contains(cellId))
					{
						clusterId = cluster.clusterId;
						break;
					}
				}
				
				if(clusterId != -1)
				{	
					
					for(ApproximatedPoint pt: cell._2.pts)
					{
						emits.add(new Tuple2<Integer, ApproximatedPoint>(clusterId, pt));
						
						//write core points
						if(pairOutputPath != null)
						{
							for(Long id : pt.ptsIds)
							{
								output = id+Conf.delimeter+clusterId+"\n";
								bw.write(output.getBytes());
							}
						}
					}
				}
			}
			
			if(pairOutputPath != null)
				bw.close();
			
			return emits.iterator();
		}
	}
	
	//Count the number of points for each cluster
	public static class CountForEachCluster implements PairFlatMapFunction<Iterator<Tuple2<Integer,ApproximatedPoint>>, Integer, Long>
	{
		@Override
		public Iterator<Tuple2<Integer, Long>> call(Iterator<Tuple2<Integer, ApproximatedPoint>> pts) throws Exception {
			// TODO Auto-generated method stub
			
			List<Tuple2<Integer, Long>> emits = new ArrayList<Tuple2<Integer, Long>>();
			
			HashMap<Integer, Long> countOfClusters = new HashMap<Integer, Long>();
			while(pts.hasNext())
			{
				Tuple2<Integer, ApproximatedPoint> pt = pts.next();
				
				if(!countOfClusters.containsKey(pt._1))
					countOfClusters.put(pt._1, (long)0);
				
				countOfClusters.put(pt._1, countOfClusters.get(pt._1)+pt._2.count);
			}
			
			for(Entry<Integer, Long> entry : countOfClusters.entrySet())
				emits.add(new Tuple2<Integer, Long>(entry.getKey(), entry.getValue()));

			return emits.iterator();
		}
	}
	
	public static class AggregateCount implements Function2<Long, Long, Long>
	{
		@Override
		public Long call(Long x, Long y) throws Exception {
			// TODO Auto-generated method stub
			return x+y;
		}
	}
	
	public static class CountEdge implements PairFunction<Tuple2<Integer,Iterable<Edge>>, Integer, Long>
	{
		@Override
		public Tuple2<Integer, Long> call(Tuple2<Integer, Iterable<Edge>> edges) throws Exception {
			// TODO Auto-generated method stub
			long numOfEdges = 0;
			for(Edge edge : edges._2)
				numOfEdges++;
			return new Tuple2(1, numOfEdges);
		}
	}
}

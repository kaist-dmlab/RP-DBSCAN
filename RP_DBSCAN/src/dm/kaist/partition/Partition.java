package dm.kaist.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import breeze.linalg.max;
import dm.kaist.algorithm.Conf;
import dm.kaist.partition.Partition.Accumulator;
import scala.Tuple2;

public class Partition implements Serializable{
	public List<Tuple2<List<Integer>, Long>> subCells;
	public int[] minCoord;
	public int[] maxCoord;
	public int dim;
	public long count = -1;
	public int partitionId;
	
	public Partition(List<Tuple2<List<Integer>, Long>> subCells, int dim)
	{
		this.partitionId = (int)System.currentTimeMillis();
		this.subCells = subCells;
		this.dim = dim;
		minCoord = new int[dim];
		maxCoord = new int[dim];
		setMinMaxCoord();
	}

	public Partition()
	{
		this.subCells = new ArrayList<Tuple2<List<Integer>, Long>>();
	}
	
	public Partition(int partitionId)
	{
		this.partitionId = partitionId;
	}
	
	public void setMinMaxCoord()
	{
		for(int i=0; i<dim; i++)
		{
			minCoord[i] = Integer.MAX_VALUE;
			maxCoord[i] = Integer.MIN_VALUE;
		}
		
		for(Tuple2<List<Integer>, Long> entry : subCells)
		{
			for(int i=0; i<dim; i++)
			{
				if(minCoord[i] > entry._1.get(i))
					minCoord[i] = entry._1.get(i);
				if(maxCoord[i] < entry._1.get(i))
					maxCoord[i] = entry._1.get(i);
			}
		}
	}
	
	public long getPtsCount()
	{
		if(count == -1)
		{
			count = 0;
			for(Tuple2<List<Integer>, Long> entry : subCells)
				count += entry._2;
			return count;
		}else
			return count;
	}
	
	//if there exist at least one splitable dimenstion, return true;
	public boolean isLargerThanMinSize()
	{		
		for(int i=0; i<dim; i++)
			if(maxCoord[i] - minCoord[i] >= 1)
				return true;
		
		return false;
	}
	
	
	
	public long[] getPtsCountOfTwoPartition(int axis, int curDim)
	{
		long[] counts = new long[2];
		counts[0] = 0;
		counts[1] = 0;
		
		for(Tuple2<List<Integer>, Long> entry : subCells)
		{
			List<Integer> cellCoords = entry._1;
			if(cellCoords.get(curDim) <= axis)
				counts[0] += entry._2;
			else
				counts[1] += entry._2;
		}
		
		return counts;
	}
	
	public List<Partition> getSubPartition(int axis, int curDim)
	{
		List<Partition> subPartitions = new ArrayList<Partition>();
		
		List<Tuple2<List<Integer>, Long>> left = new ArrayList<Tuple2<List<Integer>, Long>>();
		List<Tuple2<List<Integer>, Long>> right = new ArrayList<Tuple2<List<Integer>, Long>>();
		
		for(Tuple2<List<Integer>, Long> entry : subCells)
		{
			List<Integer> cellCoords = entry._1;
			if(cellCoords.get(curDim) <= axis)
				left.add(entry);
			else
				right.add(entry);
		}
		
		subPartitions.add(new Partition(left,this.dim));
		subPartitions.add(new Partition(right,this.dim));
		
		return subPartitions;
	}
	
	public void growingBoundaryWithEpsilon()
	{
		for(int i=0; i<dim; i++)
		{
			minCoord[i] = minCoord[i]-1;
			maxCoord[i] = maxCoord[i]+1;
		}
	}
	
	public void setPartitionId(int partitionId)
	{
		this.partitionId = partitionId;
	}
	
	public boolean isContainCell(List<Integer> cellId)
	{
		for(int i=0; i<Conf.limitDimForVirtualCombining; i++)
			if(!(cellId.get(i) >= minCoord[i] && cellId.get(i) <= maxCoord[i]))
				return false;
		return true;
	}
	
	public List<Partition> findBestSplitedPartition()
	{
		Accumulator[] accumulator = new Accumulator[dim];
		for(int i=0; i<dim; i++)
			accumulator[i] = new Accumulator();
		
		long totalPts = 0;
	
		//build statistics
		for(Tuple2<List<Integer>, Long> cell : subCells)
		{
			//dim
			for(int i=0; i<cell._1.size(); i++)
			{
				if(!accumulator[i].axies.containsKey(cell._1.get(i)))
					accumulator[i].axies.put(cell._1.get(i), (long)0);
				
				accumulator[i].axies.put(cell._1.get(i), accumulator[i].axies.get(cell._1.get(i))+cell._2 );
			}
			totalPts += cell._2;
		}
		
		for(Accumulator temp : accumulator)
			temp.refine();

		long gap = Long.MAX_VALUE;
		int bestAxis = 0;
		int bestDim = 0;
		
		for(int i=0; i<dim; i++)
		{
			long acc = 0;
			for(Entry<Integer, Long> entry : accumulator[i].axies.entrySet())
			{
				acc += entry.getValue();
				long localGap = Math.abs(Math.abs(totalPts - acc) - acc);

				if(gap > localGap)
				{
					gap = localGap;
					bestDim = i;
					bestAxis = entry.getKey();
				}
			}
		}
		return getSubPartition(bestAxis, bestDim); 
	}
	
	public class Accumulator
	{
		public TreeMap<Integer, Long> axies;
		
		public Accumulator()
		{
			this.axies = new TreeMap<Integer, Long>();
		}
		
		public void refine()
		{
			int min = Integer.MAX_VALUE;
			int max = Integer.MIN_VALUE;
			
			for(Entry<Integer, Long> entry : axies.entrySet())
			{
				if(min > entry.getKey())
					min = entry.getKey();
				if(max < entry.getKey())
					max = entry.getKey();
			}
			
			List<Integer> empty = new ArrayList<Integer>();
			for(int i=min; i<=max; i++)
			{
				if(!axies.containsKey(i))
					empty.add(i);
			}
			
			for(Integer i : empty)
				axies.put(i, (long)0);
			
		}
	}
	
	
	public class SplitPosition
	{
		public int dim;
		public int pivot; 
		

		public void setPosition(int dim, int pivot)
		{
			this.dim = dim;
			this.pivot = pivot;
		}
		
		public String toString()
		{
			return "Dim : " + this.dim +"\tPivot : "+ this.pivot;
		}
	}
}

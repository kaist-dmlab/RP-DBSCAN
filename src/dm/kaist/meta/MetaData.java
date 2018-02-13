package dm.kaist.meta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;
import dm.kaist.partition.Partition;
import dm.kaist.tree.Kdtree;
import scala.Tuple2;

import java.util.Map.Entry;

public class MetaData  implements Serializable{
	public int DIMENSION;
	public float EPSILON;
	public float APPROXIMATE;
	public int MINPTR;
	
	public int ID = 1;
	public int INDEX = 1; //x dim
	public int COUNT = 1;
	public int CHILDSTART = 1;
	public int CHILDREN = 1;
	public int BYTESOFLEVEL1;
	
	public int NUMOFLEVEL1CELL;
	public int LOWEST_LEVEL;
	public float level_1_SideLen;
	public float level_p_SideLen;
	
	public long[] encoded_Id = null;
	public int[] level_1_Meta = null;
	public char[] level_h_Meta = null;
	public int[] level_h_Count = null;
	
	//tree for neighborSearch
	public Kdtree neighborTree = null;
	public List<Kdtree> lvp_neighborTrees = null;
	
	//partition region
	public int[] minCoord;
	public int[] maxCoord;
	
	public void free()
	{
		encoded_Id = null;
		level_1_Meta = null;
		level_h_Meta = null;
		level_h_Count = null;
		lvp_neighborTrees = null;
		neighborTree = null;
		
	}

	public MetaData(int dim, float epsilon, float appr, int minPtr)
	{
		this.BYTESOFLEVEL1 = ID+INDEX*dim+COUNT+CHILDSTART+CHILDREN;
	
		this.DIMENSION = dim;
		this.EPSILON = epsilon;
		this.APPROXIMATE = appr;
		this.MINPTR = minPtr;
		
	}
	
	public void setMinMax(Partition partition, int metaBlockWindow)
	{
		minCoord = partition.minCoord;
		maxCoord = partition.maxCoord;
		int comp = (int)(Math.ceil(Math.sqrt(DIMENSION)));
		
		for(int i=0; i<DIMENSION; i++)
		{
			minCoord[i] = minCoord[i] - comp;
			maxCoord[i] = maxCoord[i] + comp;
		}
	}
	
	public long getLv1CellEncodedId(int id)
	{
		return encoded_Id[id];
	}
	
	public boolean isContainCell(List<Integer> cellId)
	{
		for(int i=0; i<DIMENSION; i++)
			if(!(cellId.get(i) >= minCoord[i] && cellId.get(i) <= maxCoord[i]))
				return false;
		return true;
	}
	
	/**
	 * MetaGeneartion using approximated cell
	 * @param id
	 * @return
	 */
	public void generateMetaDataWithApproximation(Iterable<ApproximatedCell> cells){
		//id, index, count, # of points
		//id is started from 0.
		
		int count = 0;
		
		//set min max
		minCoord = new int[DIMENSION];
		maxCoord = new int[DIMENSION];
		for(int i=0; i<DIMENSION; i++)
		{
			minCoord[i] = Integer.MAX_VALUE;
			maxCoord[i] = Integer.MIN_VALUE;
		}
		
		
		for(ApproximatedCell cell : cells)
		{
			for(int i=0; i<DIMENSION; i++)
			{
				if(minCoord[i] > cell.cellCoords.get(i))
					minCoord[i] = cell.cellCoords.get(i);
				if(maxCoord[i] < cell.cellCoords.get(i))
					maxCoord[i] = cell.cellCoords.get(i);
			}
			count++;
		}
		
		int comp = (int)(Math.ceil(Math.sqrt(DIMENSION)));
		
		for(int i=0; i<DIMENSION; i++)
		{
			minCoord[i] = minCoord[i] - comp;
			maxCoord[i] = maxCoord[i] + comp;
		}
		
		
		int id = 0;
		NUMOFLEVEL1CELL = count;// cells.size();
		encoded_Id = new long[NUMOFLEVEL1CELL];
		
		LOWEST_LEVEL = (int)(Math.ceil(1 - Math.log10(APPROXIMATE)/Math.log10(2)));
		level_1_SideLen = EPSILON / (float)Math.sqrt(DIMENSION) ;
		level_p_SideLen = EPSILON /  ((float)Math.sqrt(DIMENSION) * (1 << LOWEST_LEVEL-1));
		
		//------------------------------------------------------------
		System.out.println("LOWEST LEVEL : " + LOWEST_LEVEL);
		System.out.println("level_p_SideLen : " + level_p_SideLen);
		//------------------------------------------------------------
		
		level_1_Meta = new int[BYTESOFLEVEL1*NUMOFLEVEL1CELL];
		
		HashMap<List<Character>, Integer> levelp = new HashMap<List<Character>, Integer>();
		List<Character> temp_2_Meta = new ArrayList<Character>();
		List<Integer> temp_2_Count = new ArrayList<Integer>();

		int startIndex = 0;
		int startLvpIndex = 0;
		for(ApproximatedCell cell : cells){
			
		//-------------------------------------------	
		encoded_Id[id] = cell.cellId;
		
		startIndex = id*BYTESOFLEVEL1;
		level_1_Meta[startIndex++] = id;
				
		for(Integer it : cell.cellCoords)
			level_1_Meta[startIndex++] = it;

		//get iterable size
		int eleSize = cell.getRealPtsCount();
			
		level_1_Meta[startIndex++] = eleSize;//entry.getValue().size();
			
		//make level 2 meta info : sub coords
		//make level 2 count info : count of each child
		levelp.clear();
		for(ApproximatedPoint pt : cell.pts)
		{
			List<Character> level_p_index = pt.getLevel_p_Coords(cell.cellCoords, level_1_SideLen, level_p_SideLen, DIMENSION);
			
			if(!levelp.containsKey(level_p_index))
				levelp.put(level_p_index, pt.count);
			else
				levelp.put(level_p_index, levelp.get(level_p_index)+pt.count);
				
		}
			
		level_1_Meta[startIndex++] = startLvpIndex;
		level_1_Meta[startIndex++] = levelp.size();

		Set<Entry<List<Character>, Integer>> subEntries = levelp.entrySet();
		for(Entry<List<Character>, Integer> subEntry : subEntries)
		{
			for(Character it : subEntry.getKey())
				temp_2_Meta.add(it);
			temp_2_Count.add(subEntry.getValue());
			startLvpIndex++;
		}
		
		id++;		
		}
		
		//transform from list to []
		level_h_Meta = new char[temp_2_Meta.size()];
		for(int i=0; i<level_h_Meta.length; i++)
			level_h_Meta[i] = temp_2_Meta.get(i);
		

		level_h_Count = new int[temp_2_Count.size()];
		for(int i=0; i<level_h_Count.length; i++)
			level_h_Count[i] = temp_2_Count.get(i);
		
		//free
		temp_2_Count = null;
		temp_2_Meta = null;

	}
	
	//Position Estimation------------------------------------------------------------------------------------------
	public int getLv1CoordsPos(int id)
	{
		return id*BYTESOFLEVEL1+1;
	}
	
	//lv1의 첫 자식 시작 pos
	public int getLvpCoordsPos(int id)
	{
		return level_1_Meta[id*BYTESOFLEVEL1+DIMENSION+2]*DIMENSION;
	}
	
	//start pos of lv 1 id of lv p child 
	public int getLvpCoordsPos(int lv_1_Id, int lv_p_Id)
	{
		int startIndex = level_1_Meta[lv_1_Id*BYTESOFLEVEL1+DIMENSION+2]*DIMENSION;
		startIndex += lv_p_Id*DIMENSION;
		return startIndex;
	}
	
	public int getLvpCountPos(int id)
	{
		return level_1_Meta[id*BYTESOFLEVEL1+DIMENSION+2];
		
	}
	
	//return level 1 grid index
	public float[] getGirdCoordsIndex(int lv_1_id)
	{
		float[] result = new float[DIMENSION];
		for(int i=0; i<DIMENSION; i++)
			result[i] = level_1_Meta[getLv1CoordsPos(lv_1_id)+i];
		
		return result;
	}
	
	public List<Integer> getIntGirdCoordsIndex(int lv_1_id)
	{
		List<Integer> result = new ArrayList<Integer>();
		for(int i=0; i<DIMENSION; i++)
			result.add(level_1_Meta[getLv1CoordsPos(lv_1_id)+i]);
		
		return result;
	}
	
	//Coordinates estimation------------------------------------------------------------------------------------------
	public float getCoordsOfElement(int lv_1_Id, int lv_p_Id, int dim)
	{
		
		return level_1_Meta[getLv1CoordsPos(lv_1_Id)+dim]*level_1_SideLen + (level_h_Meta[getLvpCoordsPos(lv_1_Id, lv_p_Id)+dim]+0.5f)*level_p_SideLen;
	}
	
	public float[] getLvpCoords(int lv_1_Id, int lv_p_Id)
	{
		float[] coords = new float[DIMENSION];
		for(int i=0; i<DIMENSION; i++)
			coords[i] = getCoordsOfElement(lv_1_Id, lv_p_Id, i);
		return coords;
	}
	
	public int getLvpCount(int lv_1_Id, int lv_p_Id)
	{
		return level_h_Count[level_1_Meta[getLv1CoordsPos(lv_1_Id)+DIMENSION+1]+lv_p_Id];
	}
	
	
	public float[][] getLv1CoordsSet(int lv_1_Id)
	{
		int size = level_1_Meta[getLv1CoordsPos(lv_1_Id)+DIMENSION+2];
		float[][] set = new float[size][];
		for(int i=0; i<size; i++)
			set[i] = getLvpCoords(lv_1_Id, i);
		return set;
	}
	
	
	//neighbor tree build------------------------------------------------------------------------------------------
	public void buildNeighborSearchTree()
	{
		//build Lv1 Cell Tree
		this.neighborTree = new Kdtree(DIMENSION);
		for(int i=0; i<NUMOFLEVEL1CELL; i++)
			neighborTree.insert(i,getGirdCoordsIndex(i), 1);
		
		//build Lvp Cell Tree
		lvp_neighborTrees = new ArrayList<Kdtree>();
		for(int i=0; i<NUMOFLEVEL1CELL; i++)
		{
			int lv_1_ID = i;
			Kdtree temp = new Kdtree(DIMENSION);
			int size =  level_1_Meta[getLv1CoordsPos(lv_1_ID)+DIMENSION+2];
			for(int j=0; j<size; j++){
				float[] coords = getLvpCoords(lv_1_ID, j);
				temp.insert(0, coords, getLvpCount(lv_1_ID, j));
			}
			lvp_neighborTrees.add(temp);
		}
		
	}
	
	//neighbor tree build------------------------------------------------------------------------------------------
	public void buildNeighborSearchTreeWithoutLvp()
	{
		//build Lv1 Cell Tree
		this.neighborTree = new Kdtree(DIMENSION);
		for(int i=0; i<NUMOFLEVEL1CELL; i++)
			neighborTree.insert(i,getGirdCoordsIndex(i), 1);
	}
}

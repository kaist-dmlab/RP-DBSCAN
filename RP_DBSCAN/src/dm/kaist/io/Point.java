package dm.kaist.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dm.kaist.algorithm.Conf;
import dm.kaist.dictionary.Dictionary;

public class Point  implements Serializable{
	public long id;
	public int clusterId;
	public float[] coords;
	
	//updated values
	public boolean isCore = false;
	public int neighborPts = 0;
	
	public Point(String line, int dim)
	{
		String[] toks = line.split(Conf.delimeter);
		long id = Long.parseLong(toks[0]);
		float[] coords = new float[dim];
		for(int i=0; i<dim; i++)
			coords[i] = Float.parseFloat(toks[i+1]);
		
		this.id = id;
		this.coords = coords;
	}
	
	public Point(long id, float[] coords)
	{
		this.id = id;
		this.coords = coords;
	}
	
	public List<Integer> getLevel_1_Coords(float level_1_SideLen, int dim)
	{
		List<Integer> gridCoords = new ArrayList<Integer>();
		for(int i=0; i<dim; i++)
			gridCoords.add((int)Math.floor(this.coords[i] / level_1_SideLen));
		return gridCoords;
	}
	
	
	public List<Character> getLevel_p_Coords(List<Integer> gridCoords, float level_1_SideLen, float level_p_SideLen, int dim)
	{
		float gap = 0;
		char temp = 0;
		List<Character> key = new ArrayList<Character>();
		
		for(int i=0; i<dim; i++)
		{
			gap = gridCoords.get(i)*level_1_SideLen;
			temp = (char)(int)(Math.floor((this.coords[i]-gap)/level_p_SideLen));
			key.add(temp);
		}

		return key;
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return Arrays.equals(coords, ((Point)obj).coords);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Arrays.hashCode(coords);
	}

	public int stateWithSphere(int lv_1_Id, float sqr_r, Dictionary meta)
	{
		float closestDist = 0;
		float farthestDist = 0;
		float temp = 0;
		float temp2 = 0;
		float sqr_temp = 0;
		float sqr_temp2 = 0;
		
		for (int i = 0; i < meta.DIMENSION; i++) {
			temp = meta.level_1_Meta[meta.getLv1CoordsPos(lv_1_Id)+i]*meta.level_1_SideLen - this.coords[i];
			temp2 = temp + meta.level_1_SideLen;
			sqr_temp = temp * temp;
			sqr_temp2 = temp2 * temp2;
			if (temp >= 0) {
				// q is to the left of this grid cell in this dimension
				closestDist += sqr_temp;
			} else if (temp2 <= 0) {
				// q is to the right of this grid cell in this dimension
				closestDist += sqr_temp2;
			}
			farthestDist += (sqr_temp <= sqr_temp2 ? sqr_temp2 : sqr_temp);
		}


		if (closestDist <= sqr_r) {

			if (farthestDist <= sqr_r)
				return 1; // fully inside
			return 0; // intersect
		}
		return -1; // fully outside
	}
	
	public int stateWithSphere(List<Integer> lv_1_Coords , int dim, float sqr_r, float level_1_SideLen)
	{
		float closestDist = 0;
		float farthestDist = 0;
		float temp = 0;
		float temp2 = 0;
		float sqr_temp = 0;
		float sqr_temp2 = 0;
		
		for (int i = 0; i < dim; i++) {
			temp = lv_1_Coords.get(i)*level_1_SideLen - this.coords[i];
			temp2 = temp + level_1_SideLen;
			sqr_temp = temp * temp;
			sqr_temp2 = temp2 * temp2;
			if (temp >= 0) {
				// q is to the left of this grid cell in this dimension
				closestDist += sqr_temp;
			} else if (temp2 <= 0) {
				// q is to the right of this grid cell in this dimension
				closestDist += sqr_temp2;
			}
			farthestDist += (sqr_temp <= sqr_temp2 ? sqr_temp2 : sqr_temp);
		}


		if (closestDist <= sqr_r) {

			if (farthestDist <= sqr_r)
				return 1; // fully inside
			return 0; // intersect
		}
		return -1; // fully outside
	}
	
	public String toString()
	{
		String result = "";
		for(int i=0; i<coords.length; i++)
		{
			if(i != coords.length-1)
				result += coords[i] +",";
			else
				result += coords[i];
		}
		
		return result;
	}
	
}

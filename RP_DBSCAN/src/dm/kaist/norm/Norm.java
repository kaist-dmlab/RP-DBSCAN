package dm.kaist.norm;

import java.io.Serializable;
import java.util.List;

public class Norm implements Serializable {
	public static float sqr_L2_norm(float[] coords1, float[] coords2)
	{
		float temp = 0;
		for(int i=0; i<coords1.length; i++)
			temp += (coords1[i] - coords2[i])* (coords1[i] - coords2[i]);
		return temp;
	}
	
	public static boolean checkWithinBoundary(float[] coords1, float[] coords2, float comp)
	{
		boolean temp = true;
		float gap = 0;
		float sum = 0;
		for(int i=0; i<coords1.length; i++)
		{
			gap = coords1[i]-coords2[i];
			if(gap < 0 )
				gap *= -1;
			
			sum += gap;
			if(gap > comp)
				return false;
		}
	
		if(sum == 0)
			return false;
		
		return temp;
	}
	
	public static float sqr_L2_norm(List<Integer> coords1, List<Integer> coords2)
	{
		float dist = 0;
		for(int i=0; i<coords1.size(); i++)
		{
			float temp = ((float)coords1.get(i) - (float)coords2.get(i));
			dist += temp*temp;
		}
		return dist;
	}
}

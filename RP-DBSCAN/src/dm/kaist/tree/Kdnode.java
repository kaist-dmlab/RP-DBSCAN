package dm.kaist.tree;

import java.io.Serializable;
import java.util.Arrays;

public class Kdnode implements Serializable {
    Kdnode left = null;
	Kdnode right = null;
	Kdnode parent = null;
	
	int id;
	public float[] coords;
	public int count;
	int level;
	
	public Kdnode(){
		
	}
	
	public Kdnode(Kdnode parent, int level, int id, float[] coords, int count){
		this.level = level;
		this.id = id;
		this.coords = coords;
		this.count = count;
	}
	
	public String toString()
	{
		return Arrays.toString(coords);
	}
	
	public void free()
	{
		coords = null;
	}
	
}

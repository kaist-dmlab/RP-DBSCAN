package dm.kaist.graph;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Edge  implements Serializable{
	public long u;
	public long v;
	public float weight=0;
	public boolean isCore = false;
	


	public Edge(long u, long v)
	{
		this.u = u;
		this.v = v;
		this.weight = 0;
	}
	
	public Edge(long u, long v, float weight)
	{
		this.u = u;
		this.v = v;
		this.weight = weight;
	}
	
	public void setCore(boolean bool)
	{
		isCore = bool;
	}
	
	public String toString()
	{
		return this.u+" "+this.v;
	}
	
	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		
		Edge other = (Edge)(arg0);
		
		if((u == other.u && v == other.v) || (u == other.v && v == other.u))
			return true;
		else
			return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		
		List<Long> hash = new ArrayList<Long>();
		if(u<v)
		{
			hash.add(u);
			hash.add(v);
		}else
		{
			hash.add(v);
			hash.add(u);
		}
		
		
		return hash.hashCode();
	}
}

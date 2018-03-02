package dm.kaist.graph;

import java.io.Serializable;
import java.util.List;

import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;
import dm.kaist.tree.Kdtree;

public class LabeledCell  implements Serializable{
	public int clusterId = -1;
	public Long cellId = null;
	public Iterable<ApproximatedPoint> pts = null;
	public Kdtree tree = null;
	
	public LabeledCell(int clusterId, Long cellId, Iterable<ApproximatedPoint> pts)
	{
		this.clusterId = clusterId;
		this.cellId = cellId;
		this.pts = pts;
	}
	
	public LabeledCell(int clusterId, Long cellId)
	{
		this.clusterId = clusterId;
		this.cellId = cellId;
	}
	
	public void setPts(Iterable<ApproximatedPoint> pts)
	{
		this.pts = pts;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return cellId.hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return cellId.toString();
	}
	
	public void buildKdTree(int dim)
	{
		tree = new Kdtree(dim);
		int id = 0;
		for(ApproximatedPoint pt : pts)
		{
			tree.insert(id++, pt.coords, 1);
		}
	}
}

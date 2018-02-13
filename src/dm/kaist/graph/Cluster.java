package dm.kaist.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Cluster implements Serializable {
	public int clusterId;
	public HashSet<Long> coreCellIds;
	public HashMap<Long,List<Long>> borderCellIds;

	public Cluster(int clusterId)
	{
		this.clusterId = clusterId;
		this.coreCellIds = new HashSet<Long>();
		this.borderCellIds = new HashMap<Long,List<Long>>();
	}
	
	public Cluster(int clusterId, HashSet<Long> coreCellIds)
	{
		this.clusterId = clusterId;
		this.coreCellIds = coreCellIds;
		this.borderCellIds = new HashMap<Long,List<Long>>();
	}
}

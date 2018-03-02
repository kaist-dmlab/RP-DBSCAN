package dm.kaist.dictionary;

import java.io.Serializable;
import java.util.List;

import dm.kaist.tree.Kdtree;

public class NeighborCell implements Serializable {
	public List<Integer> cellId = null;
	public Kdtree lv_p_kdtree = null;
	public int t;
	
	public NeighborCell(List<Integer> cellId, Kdtree lv_p_kdtree)
	{
		this.cellId = cellId;
		this.lv_p_kdtree = lv_p_kdtree;
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		NeighborCell objCell = (NeighborCell) obj;
		
		if(cellId.equals(objCell.cellId))
			return true;
		else
			return false;
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
}

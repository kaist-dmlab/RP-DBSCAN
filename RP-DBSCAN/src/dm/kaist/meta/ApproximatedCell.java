package dm.kaist.meta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import breeze.util.Iterators;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;

public class ApproximatedCell implements Serializable {
	
	public long cellId = Long.MAX_VALUE;
	public List<Integer> cellCoords = null;
	public int count;
	public List<ApproximatedPoint> pts = null;
	public boolean ifFullCore = false;
	
	public ApproximatedCell(List<Integer> cellCoords) {
		this.cellCoords = cellCoords;
		pts = new ArrayList<ApproximatedPoint>();
		this.count = 0;
		// TODO Auto-generated constructor stub
	}

	public void addPoint(ApproximatedPoint pt) {
		// TODO Auto-generated method stub
		
		if(cellId > (int)pt.id)
			cellId = (int)pt.id;
		pts.add(pt);
		
		this.count += pt.count;
	}
	
	
	public int getApproximatedPtsCount()
	{
		return this.pts.size();
	}
	
	public int getRealPtsCount() {
	
		return count;
	}

}

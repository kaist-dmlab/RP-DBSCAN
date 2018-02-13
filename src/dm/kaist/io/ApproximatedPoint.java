package dm.kaist.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dm.kaist.algorithm.Conf;
import dm.kaist.meta.MetaData;

public class ApproximatedPoint extends Point implements Serializable{

	public List<Long> ptsIds = null;
	public int count = 0;
	
	public ApproximatedPoint(String line, int dim) {
		super(line, dim);
		// TODO Auto-generated constructor stub
	}
	
	
	public ApproximatedPoint(long id, float[] coords)
	{
		super(id, coords);
		this.id = id;
		this.coords = coords;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return Arrays.equals(this.coords, ((Point)obj).coords);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Arrays.hashCode(this.coords);
	}

	
}

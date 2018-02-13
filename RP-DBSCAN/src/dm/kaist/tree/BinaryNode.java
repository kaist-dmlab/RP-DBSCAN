package dm.kaist.tree;

import java.io.Serializable;
import java.util.List;

import dm.kaist.graph.Edge;

public class BinaryNode implements Serializable {
	BinaryNode parent = null;
	//large
	BinaryNode left = null;
	//ged
	BinaryNode right = null;
	
	public float weight;
	
	public Edge edge = null;
	
	public BinaryNode(BinaryNode parent, BinaryNode left, BinaryNode right, Edge edge, float weight)
	{
		this.parent = parent;
		this.left = left;
		this.right = right;
		this.edge = edge;
		this.weight = weight;
	}
}

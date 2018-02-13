package dm.kaist.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import dm.kaist.norm.Norm;

public class Kdtree implements Serializable {
	
	public Kdnode root;
	public int count;
	public int dim;
	public int leafLevel=0;
	
	public Kdtree(){
		this.count = 0;
		this.root = null;
	}
	
	public Kdtree(int dim)
	{
		this.count = 0;
		this.dim = dim;
		this.root = null;
	}
	
	public void clear()
	{
		root = null;
		count = 0;
		leafLevel = 0;
	}

	public void insert(int id, float[] coords, int node_count)
	{
		if(this.root == null){
			this.root = new Kdnode(null, 0, id, coords, node_count);
			this.count += node_count;
		}else
			addNode(id, coords, node_count);
	}
	
	public void addNode(int id, float[] coords, int node_count, Kdnode curNode)
	{
		int curLevel = curNode.level;
		int target = curLevel%(this.dim);
		
		if(coords[target] >= curNode.coords[target])
		{
			if(curNode.left == null)
			{
				Kdnode newNode = new Kdnode(curNode, curLevel+1, id, coords, node_count);
				this.count += node_count;
				curNode.left = newNode;
				leafLevel = Math.max(leafLevel, curLevel+1);
			}else
				addNode(id, coords, node_count, curNode.left);
		}else{
			if(curNode.right == null)
			{
				Kdnode newNode = new Kdnode(curNode, curLevel+1, id, coords, node_count);
				this.count += node_count;
				curNode.right = newNode;
				leafLevel = Math.max(leafLevel, curLevel+1);
			}else
				addNode(id, coords, node_count, curNode.right);
		}
	}

	public void addNode(int id, float[] coords, int node_count)
	{
		Kdnode curNode = this.root;

		while(true)
		{
			int curLevel = curNode.level;
			int target = curLevel%(this.dim);
			
			if(coords[target] >= curNode.coords[target])
			{
				if(curNode.left == null)
				{
					int level = curNode.level+1;
					Kdnode newNode = new Kdnode(curNode, level, id, coords, node_count);
					this.count += node_count;
					leafLevel = Math.max(leafLevel, level);
					curNode.left = newNode;
					break;
				}else
					curNode = curNode.left;
			}else{
				if(curNode.right == null)
				{
					int level = curNode.level+1;
					Kdnode newNode = new Kdnode(curNode, level, id, coords, node_count);
					this.count += node_count;
					leafLevel = Math.max(leafLevel, level);	
					curNode.right = newNode;
					break;
				}else
					curNode = curNode.right;
			}
		}
	}
	
	public Kdnode partialClosestNode(Kdnode start, float[] coords, float curBest, float sqr_r)
	{
		//init minDist = Double.MAX_VALUE
		//init start = root
		float minDist = curBest;
		Kdnode init = start;
		Kdnode result = null;
		
		Kdnode next = null;
		while(true)
		{
			float dist = sqrDist(coords, start.coords, this.dim);
			
			if(dist <= sqr_r)
				return new Kdnode(null, -1, -1, null, -1);

			
			if(dist <= minDist)
			{
				minDist = dist;
				result = start;
			}

			int curLevel = start.level;
			int target = curLevel%(this.dim);
			
			next = null;
			if(coords[target] >= start.coords[target])
				next = start.left;
			else
				next = start.right;

			if(next == null)
				break;
			else
				start = next;

		}	
		
		if(result == null)
			return init.parent;
		else
			return result;
	}
	
	public Kdnode cloestNode(float[] coords, float sqr_r)
	{
		Kdnode start = this.root;
		float minDist = Float.MAX_VALUE;
		
		Kdnode next = null;
		Kdnode result = null;
	
		do{
			next = partialClosestNode(start, coords, minDist, sqr_r);
			if(next.count == -1)
				return null;
			
			if(next != null)
			{
				result = next;
				int curLevel = next.level;
				int target = curLevel%(this.dim);
				if(coords[target] >= next.coords[target])
				{
					if(next.right == null)
						break;	
					start = next.right;
				}
				else
				{
					if(next.left == null)
						break;	
					start = next.left;
				}
				
			}
		}while(next != null);
		return result;
	}
	
	
	public float sqrDist(float[] coords, float[] coords2, int dim)
	{
		float dist = 0;
		for(int i=0; i<dim; i++)
		{
			float temp = coords[i]-coords2[i];
			dist += temp*temp;
		}
		return dist;
	}
	
	
	public void getNeighborId(Kdnode startNode, float[] coords, List<Integer> neighbors, int comp)
	{	
		if(startNode == null)
			return;
		
		int curLevel = startNode.level;
		int target = curLevel%(this.dim);
	
		{
			float gap = coords[target] - startNode.coords[target];
			
			if(gap >= 0 && Math.abs(gap) > comp)
			{
				if(startNode.left != null)
					getNeighborId(startNode.left, coords, neighbors, comp);
			}else if(gap < 0 && Math.abs(gap) > comp){
				if(startNode.right != null)
					getNeighborId(startNode.right, coords, neighbors, comp);
			}else{
				if(Norm.checkWithinBoundary(coords, startNode.coords, comp))
					neighbors.add(startNode.id);
				if(startNode.left != null)
					getNeighborId(startNode.left, coords, neighbors, comp);
				if(startNode.right != null)
					getNeighborId(startNode.right, coords, neighbors, comp);
			}
		}
	}
	
	public void getNeighborNode(Kdnode startNode, float[] coords, List<Kdnode> neighbors, float comp)
	{	
		int curLevel = startNode.level;
		int target = curLevel%(this.dim);
		

		{
			float gap = coords[target] - startNode.coords[target];
			
			if(gap >= 0 && Math.abs(gap) > comp)
			{
				if(startNode.left != null)
					getNeighborNode(startNode.left, coords, neighbors, comp);
			}else if(gap < 0 && Math.abs(gap) > comp){
				if(startNode.right != null)
					getNeighborNode(startNode.right, coords, neighbors, comp);
			}else{
				if(Norm.checkWithinBoundary(coords, startNode.coords, comp))
					neighbors.add(startNode);
				if(startNode.left != null)
					getNeighborNode(startNode.left, coords, neighbors, comp);
				if(startNode.right != null)
					getNeighborNode(startNode.right, coords, neighbors, comp);
			}
		}
	}
	
	public String toString()
	{
		String result = "";
		if(root != null)
		{
			Stack<Kdnode> stack = new Stack<Kdnode>();
			stack.push(root);
			
			while(!stack.isEmpty())
			{
				Kdnode cur = stack.pop();
				
				result += cur.toString()+"\n";
				
				if(cur.left != null) stack.push(cur.left);
				if(cur.right != null) stack.push(cur.right);
			}	
		}else
			return "no node!";
		
		return result;
	}

}

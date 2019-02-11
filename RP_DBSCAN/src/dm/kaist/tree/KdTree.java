package lib.dm.kaist.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import lib.dm.kaist.norm.Norm;

public class KdTree implements Serializable {
	
	public KdNode root;
	public int count;
	public int dim;
	public int leafLevel=0;
	
	public KdTree(){
		this.count = 0;
		this.root = null;
	}
	
	public KdTree(int dim)
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
			this.root = new KdNode(null, 0, id, coords, node_count);
			this.count += node_count;
		}else
			addNode(id, coords, node_count);
		
	}
	
	public void addNode(int id, float[] coords, int node_count)
	{
		KdNode curNode = this.root;

		while(true)
		{
			int curLevel = curNode.level;
			int target = curLevel%(this.dim);
			
			if(coords[target] >= curNode.coords[target])
			{
				if(curNode.left == null)
				{
					int level = curNode.level+1;
					KdNode newNode = new KdNode(curNode, level, id, coords, node_count);
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
					KdNode newNode = new KdNode(curNode, level, id, coords, node_count);
					this.count += node_count;
					leafLevel = Math.max(leafLevel, level);	
					curNode.right = newNode;
					break;
				}else
					curNode = curNode.right;
			}
		}
	}
	

	public KdNode partialClosestNode(KdNode start, float[] coords, float[] mins, float sqr_r)
	{
		KdNode init = start;
		KdNode result = null;
		
		KdNode next = null;
		while(true)
		{
			float dist = sqrDist(coords, start.coords, this.dim);
			
			if(dist <= sqr_r)
			{	
				return new KdNode(null, -1, -1, null, -1);
			}
			
			
			if(dist <= mins[0])
			{
				mins[0] = dist;
				result = start;
			}
			
			if(dist <= mins[1])
				mins[1] = dist;

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
		
		return result;
	}
	
	public KdNode cloestNode(float[] coords, float sqr_r)
	{
		KdNode start = this.root;
		KdNode next = null;
		KdNode result = null;
		
		KdNode global_result = null;

		float[] mins = new float[2];
		//local min
		mins[0] = Float.MAX_VALUE;
		//global min
		mins[1] = Float.MAX_VALUE;
		
		do{
			mins[0] = Float.MAX_VALUE;
			next = partialClosestNode(start, coords, mins, sqr_r);
			if(next.count == -1)
				return null;
			
			if(mins[0] == mins[1]) 
				global_result = next;
			
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
		return global_result;
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
	

	public void getNeighborId(float[] coords, List<Integer> neighbors, int comp)
	{	
		
		Stack<KdNode> stack = new Stack<KdNode>();
		stack.push(this.root);
		
		while(!stack.isEmpty())
		{
			KdNode startNode = stack.pop();
			
			if(startNode == null)
				continue;
			
			int curLevel = startNode.level;
			int target = curLevel%(this.dim);
	
			
			float gap = coords[target] - startNode.coords[target];
			
			if(gap >= 0 && Math.abs(gap) > comp)
			{
				if(startNode.left != null)
					stack.push(startNode.left);
			}else if(gap < 0 && Math.abs(gap) > comp){
				if(startNode.right != null)
					stack.push(startNode.right);
			}else{
				if(Norm.checkWithinBoundary(coords, startNode.coords, comp))
					neighbors.add(startNode.id);
				if(startNode.left != null)
					stack.push(startNode.left);
				if(startNode.right != null)
					stack.push(startNode.right);
			}
		}	
	}
	
	public void getNeighborNode(float[] coords, List<KdNode> neighbors, float comp)
	{	
		
		Stack<KdNode> stack = new Stack<KdNode>();
		stack.push(this.root);
		
		while(!stack.isEmpty())
		{
			KdNode startNode = stack.pop();
			int curLevel = startNode.level;
			int target = curLevel%(this.dim);
	
			
			float gap = coords[target] - startNode.coords[target];
			
			if(gap >= 0 && Math.abs(gap) > comp)
			{
				if(startNode.left != null)
					stack.push(startNode.left);
			}else if(gap < 0 && Math.abs(gap) > comp){
				if(startNode.right != null)
					stack.push(startNode.right);
			}else{
				if(Norm.checkWithinBoundary(coords, startNode.coords, comp))
					neighbors.add(startNode);
				if(startNode.left != null)
					stack.push(startNode.left);
				if(startNode.right != null)
					stack.push(startNode.right);
			}
		}	
	}
	
	public String toString()
	{
		String result = "";
		
		if(root != null)
		{
			Stack<KdNode> stack = new Stack<KdNode>();
			stack.push(root);
			
			while(!stack.isEmpty())
			{
				
				
				KdNode cur = stack.pop();
				
				result += cur.toString()+"\n";
				
				if(cur.left != null) stack.push(cur.left);
				if(cur.right != null) stack.push(cur.right);

			}
			
			
			
			
			
		}else
			return "no node!";
		
		return result;
	}

}

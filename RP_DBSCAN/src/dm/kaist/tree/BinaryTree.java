package dm.kaist.tree;

import java.io.Serializable;
import java.util.List;

import dm.kaist.graph.Edge;

public class BinaryTree  implements Serializable {
	
	public BinaryNode root;
	public int count;
	
	public BinaryTree()
	{
		count = 0;
	}
	
	public boolean isEmpty()
	{
		if(root == null)
			return true;
		else
			return false;
	}
	public void insertNode(Edge edge, float weight)
	{
		if(root == null)
		{
			root = new BinaryNode(null, null, null, edge, weight);
			count++;
		}
		else
		{
			BinaryNode curNode = root;
	
			while(true)
			{
				if(curNode.weight <= weight)
				{
					if(curNode.right == null)
					{
						curNode.right = new BinaryNode(curNode, null, null, edge, weight);
						count++;
						break;
					}
					else
					{
						curNode = curNode.right;
						continue;
					}
				}else
				{
					if(curNode.left == null)
					{
						curNode.left = new BinaryNode(curNode, null, null, edge, weight);
						count++;
						break;
					}
					else
					{
						curNode = curNode.left;
						continue;
					}
				}
			}	
		}
	}
	
	public BinaryNode popMinimumNode()
	{
		if(root == null)
		{
			System.err.println("No node in tree!");
			return null;
		}
		
		BinaryNode minNode = root;
		
		while(minNode.left != null)
		{
			minNode = minNode.left;
		}
		
		
		//remove
		if(minNode.parent != null)
		{
			if(minNode.right != null)
			{
				minNode.parent.left = minNode.right;
				minNode.right.parent = minNode.parent;
			}
			else
				minNode.parent.left = null;
		}
		else
		{
			if(minNode.right != null)
			{
				this.root = minNode.right;
				minNode.right.parent = null;
			}else
				this.root = null;
			
		}
		
		return minNode;
	}
	
	
}

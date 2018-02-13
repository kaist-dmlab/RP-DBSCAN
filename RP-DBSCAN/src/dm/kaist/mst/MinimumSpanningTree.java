package dm.kaist.mst;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Set;

import dm.kaist.algorithm.Conf;
import dm.kaist.graph.Cluster;
import dm.kaist.graph.Edge;
import dm.kaist.io.FileIO;
import dm.kaist.tree.BinaryNode;
import dm.kaist.tree.BinaryTree;
import scala.Tuple2;

public class MinimumSpanningTree {
	public List<Edge> BuildMinimumSpanningForest(HashMap<Long, List<Edge>> coreEdgeSet, List<Cluster> clusters)
	{
		List<Edge> updatedEdge = new ArrayList<Edge>();
		
		int clusterId = 0;
		boolean isNextCluster = false;
		Long bridge = null;
		List<Edge> edgeList = null;
		
		BinaryTree possibleEdge = new BinaryTree();
		HashSet<Long> clusterMap = new HashSet<Long>();
		
		while(!coreEdgeSet.isEmpty())
		{
			//System.out.println("\tcluster : "+ clusterId);
			int cur = 1;
			//select randomly start node.
			Entry<Long, List<Edge>> initEntry = coreEdgeSet.entrySet().iterator().next();
			coreEdgeSet.remove(initEntry.getKey());
			
			//add init cluster instance
			clusterMap.add(initEntry.getKey());
				
			edgeList = null;
			bridge = null;
			isNextCluster = false;
			List<Edge> edges = new ArrayList<Edge>();
			
			while(true)
			{
				//firstime when we don't know bridge.
				if(bridge == null)
					edgeList = initEntry.getValue();
				else
				{
					edgeList = coreEdgeSet.get(bridge);
					coreEdgeSet.remove(bridge);
				}
				
				//add possible edge of current cluster
				for(Edge edge : edgeList)
				{
					//should check, cycle is occured
					if(!clusterMap.contains(edge.v))
						possibleEdge.insertNode(edge, edge.weight);
				}
		
				boolean isCycle = true;
				BinaryNode minNode = null;
				
				//should check, cycle is occured
				while(isCycle)
				{
					
					if(possibleEdge.isEmpty())
					{
						//tree is emy
						isNextCluster = true;	
						break;
					}
						
					minNode = possibleEdge.popMinimumNode();
					if(!clusterMap.contains(minNode.edge.v))
						isCycle = false;
				}
					
				//if isNextClsuter then break.
				if(isNextCluster)
				{
					clusters.add(new Cluster(clusterId++, clusterMap));
					bridge = null;
					clusterMap = new HashSet<Long>();
					break;
				}
					
				Edge minEdge = minNode.edge;
					
				Long v = minEdge.v;
						
				//add next cluster instance
				clusterMap.add(v);
				//System.out.println(cur);
				cur++;
						
				//add connectedEdge for cluster
				edges.add(minEdge);
					
				bridge = v;	
			}
			
			
			for(Edge edge : edges)
			{
				updatedEdge.add(edge);
			//	Edge inverseEdge = new Edge(edge.v, edge.u, edge.weight);
			//	inverseEdge.setCore(true);
			//	updatedEdge.add(inverseEdge);
			}
			
		}

		return updatedEdge;
	}
		
	public List<Tuple2<Integer, Edge>> reduceEdgesByMST(HashSet<Long> mergedCoreCells, HashSet<Edge> edges, int numOfPartition)
	{

		for(Edge edge : edges)
		{
			if( mergedCoreCells.contains(edge.u) && mergedCoreCells.contains(edge.v))
					edge.setCore(true);
		}
		
		
		HashMap<Long, List<Edge>> coreEdges = new HashMap<Long, List<Edge>>();
		List<Edge> unknownEdges = new ArrayList<Edge>();
		
		//divide edges to core edges and unknown edges
		for(Edge edge : edges)
		{
			if(edge.isCore)
			{
				if(!coreEdges.containsKey(edge.u))
					coreEdges.put(edge.u, new ArrayList<Edge>());	
				coreEdges.get(edge.u).add(edge);
				
				if(!coreEdges.containsKey(edge.v))
					coreEdges.put(edge.v, new ArrayList<Edge>());	
				
				Edge inverseEdge = new Edge(edge.v, edge.u, edge.weight);
				inverseEdge.setCore(true);
				coreEdges.get(edge.v).add(inverseEdge);
				
			}else
				unknownEdges.add(edge);
		}

		
		List<Cluster> clusters = new ArrayList<Cluster>();
		
		
		List<Edge> updatedEdge = BuildMinimumSpanningForest(coreEdges, clusters);
		
		//without 1iteration
		//List<Edge> updatedEdge = new ArrayList<Edge>();
		//for(Entry<Long, List<Edge>> entry : coreEdges.entrySet())
		//	updatedEdge.addAll(entry.getValue());
		
		updatedEdge.addAll(unknownEdges);


		List<Tuple2<Integer, Edge>> emits = new ArrayList<Tuple2<Integer,Edge>>();
		for(Edge edge : updatedEdge)
			emits.add(new Tuple2<Integer, Edge>((int)(Math.random()*numOfPartition), edge));
		
		return emits;
		
	}
}

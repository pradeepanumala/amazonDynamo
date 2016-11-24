package node;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import utility.Utility;



//Every NodeId is storing the Max hash key it can handle
public class PreferenceList { 
	public LinkedList<PreferenceListNode> nodesList;
	private int N;
	
	public PreferenceList(int N){
		nodesList= new LinkedList<PreferenceListNode>();
		this.N = N;
	}

	public int addNode(int NodeId, BigInteger NodeHash){
		Utility.trace("Adding the node " + NodeId+"to the preference list");
		try{
			int i;
			PreferenceListNode newNode = new PreferenceListNode( NodeId, NodeHash);
			i = 0;
			if(nodesList != null){
				while(i < nodesList.size()){
					int result = NodeHash.compareTo( nodesList.get(i).getNodeHash());
					if(result < 0){
						break;
					}
					else if(result == 0){
						return 0;
					}
					i++;
				}
				nodesList.add(i, newNode);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}

	
	/*Return the top N nodes in the list for the given key*/
	public List<Integer> getTopN(BigInteger Key){
		List<Integer> topNList = new ArrayList<Integer>();
		try{
			int i;
			i = 0;
			while(i < nodesList.size()){
				if(Key.compareTo( nodesList.get(i).getNodeHash()) < 0){
					break;
				}
				i++;
			}
			int j =0;
			
			while(j < N && i<nodesList.size()){
				topNList.add(nodesList.get(i).getNodeId());
				j++;
				i++;
			}
			
			if(j<N){
				i = 0;
				while(j < N && i<nodesList.size()){
					topNList.add(nodesList.get(i).getNodeId());
					j++;
					i++;
				}
			}
	
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return topNList;
	}
	
	public void deleteNode(int NodeId){

		int size=nodesList.size();
		for(int i=0;i<size;i++){
			if(nodesList.get(i).getNodeId()==NodeId){
				nodesList.remove(i);
				break;
			}
		}
	}
	
	public int getTopNode(BigInteger Key){
		try{
			int i;
			i = 0;
			while(i < nodesList.size()){
				if(Key.compareTo( nodesList.get(i).getNodeHash()) < 0){
					break;
				}
				i++;
			}
			if(i == nodesList.size())
				return nodesList.get(0).getNodeId();
			else
				return nodesList.get(i).getNodeId();
	
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return  -1;
	}
	
	public int getNextAvailableNode(int startNodeId, int lastNodeid){
		int i;
		i = 0;
		while(i < nodesList.size()){
			if( nodesList.get(i).getNodeId() == lastNodeid){
				break;
			}
			i++;
		}
		if(nodesList.get((++i % nodesList.size())).getNodeId() != startNodeId){
			return nodesList.get(i% nodesList.size()).getNodeId();
		}
		return -1;
		
	}
	
	public int getNextNode(int NodeId){
		int i = 0;
		while(i < nodesList.size()){
			if( nodesList.get(i).getNodeId() == NodeId){
				break;
			}
			i++;
		}
		i++;
		i=i%nodesList.size();
		return nodesList.get(i).getNodeId();
	}
}

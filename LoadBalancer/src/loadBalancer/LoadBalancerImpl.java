package loadBalancer;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.ConnectException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import node.INode;
import protobuf.LoadBalancerProto;
import protobuf.DynamoProto.GetRequest;
import protobuf.DynamoProto.GetResponse;
import protobuf.DynamoProto.PutResponse;
import protobuf.LoadBalancerProto.BInteger;
import protobuf.LoadBalancerProto.ClusterMembers;
import protobuf.LoadBalancerProto.Config;
import protobuf.LoadBalancerProto.NodeDetails;

 
@SuppressWarnings("serial")
public class LoadBalancerImpl extends UnicastRemoteObject implements ILoadBalancer   {
	static ArrayList<NodeInfo>  nodesList = new ArrayList<NodeInfo>();
	static ArrayList<Integer> nodeIdList = new ArrayList<Integer>();
	static int CurrentIndex=0;
	static int R=2;
	static int W=2;
	static int N=3;
	static final int TFail = 60;
	static final int TCleanUp =75;
	static final int TGossip = 2;
	static final int LBInformerCount=2;
	static AtomicInteger Id =new AtomicInteger(100);
	static final int LoadBalancerPort=1099;
	static final String LoadBalancerIp="10.0.0.1";

	static HashMap<String, Integer> deletedNodesMap = new HashMap<String, Integer>();
	
	protected LoadBalancerImpl() throws RemoteException {
		super();
	}
	class NodeInfo{
		String Ip = "";
		int Port;
		BigInteger NodeHash;
		int NodeId;
		public NodeInfo(String Ip,int Port,BigInteger NodeHash,int NodeId){
			this.Ip=Ip;
			this.Port=Port;
			this.NodeHash=NodeHash;
			this.NodeId=NodeId;
		}
	}
	
	class HashComparator implements Comparator<NodeInfo>{
		public int compare(NodeInfo n1, NodeInfo n2) {
			return n1.NodeHash.compareTo(n2.NodeHash);
		}
	}
	
	public static void main(String args[]){
		try {
			Registry registry = null;
			try {
			    registry = LocateRegistry.getRegistry(LoadBalancerPort);
			    registry.list();
			}
			catch (RemoteException e) { 
			    registry = LocateRegistry.createRegistry(LoadBalancerPort);
			}
			
			ILoadBalancer iLoadBalancer = new LoadBalancerImpl();
			Naming.rebind("rmi://"+LoadBalancerIp+":"+LoadBalancerPort+"/loadBalancer", iLoadBalancer);
			System.out.println("LoadBalancer started..");
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
	}
	
	//@Override
	public byte[] addNode(String Ip,int Port) {
		 LoadBalancerProto.ClusterMembers.Builder cm = ClusterMembers.newBuilder();
		 
		// Set all the configuration details
		Config.Builder configBuilder = Config.newBuilder();
		configBuilder.setN(N);
		configBuilder.setW(W);
		configBuilder.setR(R);
		configBuilder.setLBInformerCount(LBInformerCount);
		configBuilder.setTCleanUp(TCleanUp);
		configBuilder.setTFail(TFail);
		configBuilder.setTGossip(TGossip);
		
		NodeDetails.Builder ndb= NodeDetails.newBuilder();
		int myNodeId;
		BigInteger NodeHash=getNodeHash(Ip,Port);
		int respNodeIndex= getRespNode(NodeHash);
		if(!deletedNodesMap.containsKey(Ip + Port)){
			myNodeId=Id.incrementAndGet();
			if(nodesList.size()>=N){
				
				boolean status=addNodeInMiddle(Ip, Port, NodeHash, respNodeIndex );
				if(!status){
					System.out.println("Failed to add node in the middle");
					cm.setStatus(false);
					return cm.build().toByteArray();
				}
			}
		}else{
			myNodeId = deletedNodesMap.get(Ip + Port);
			hintedHandoff(Ip, Port, NodeHash,myNodeId, respNodeIndex);
		}
		
		
		BInteger.Builder builder = BInteger.newBuilder();
		ByteString bytes=ByteString.copyFrom(NodeHash.toByteArray());
		builder.setValue(bytes);
		
		ndb.setIp(Ip);
		ndb.setPort(Port);
		ndb.setNodeId(myNodeId);
		ndb.setNodeHash(builder.build());
		
		nodesList.add(new NodeInfo(Ip,Port,NodeHash,myNodeId));
		nodeIdList.add(myNodeId);
		Collections.sort(nodesList, new HashComparator() );
		deletedNodesMap.remove(Ip+Port);
		
		for(int i=0;i<nodesList.size();i++){
			System.out.print( nodesList.get(i).NodeId+"-");
			NodeDetails.Builder ndb1= NodeDetails.newBuilder();
			ndb1.setIp(nodesList.get(i).Ip);
			ndb1.setPort(nodesList.get(i).Port);
			ndb1.setNodeId(nodesList.get(i).NodeId);
			
			BInteger.Builder b = BInteger.newBuilder();
			ByteString by=ByteString.copyFrom(nodesList.get(i).NodeHash.toByteArray());
			b.setValue(by);
			
			ndb1.setNodeHash(b.build());
			cm.addClusterMems(ndb1);
		}

		cm.setStatus(true);
		cm.addClusterMems(ndb);
		cm.setConfigDetails(configBuilder);
		cm.setMyNodeId(myNodeId);
		cm.setMyNodeHash(builder.build());
		return cm.build().toByteArray();
	}
	
	private void hintedHandoff(String newNodeIP, int newNodePort, BigInteger newNodeHash, int newNodeId, int respNodeIndex) {
		INode inode = null;
		boolean status = false;
		try {
			System.out.println("Calling the node "+nodesList.get(respNodeIndex).Ip+":"+nodesList.get(respNodeIndex).Port+" for hinted handoff");
			inode=(INode)Naming.lookup("rmi://"+nodesList.get(respNodeIndex).Ip + ":" + nodesList.get(respNodeIndex).Port + "/nodeServer");
			status = inode.hintedHandoff(newNodeIP, newNodePort, newNodeHash, newNodeId,true);
			if(status){
				System.out.println("Hinted Handoff done successfully");
			}
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		} 
	} 

	private BigInteger getNodeHash(String Ip, int Port){
		BigInteger nodeHash = new Utility().md5(Ip+Port);
		return nodeHash;
	}

	private int getRespNode(BigInteger newNodeHash){
			
				int i=0;
				while(i < nodesList.size()){
					if(newNodeHash.compareTo( nodesList.get(i).NodeHash) <= 0){
						break;
					}
					i++;
				}
				if(i == nodesList.size())
					return 0;
				else
					return i;
	}
	
	private boolean addNodeInMiddle(String newNodeIP, int newNodePort, BigInteger newNodeHash, int respNodeIndex){
		//Call the AddNode() on a node already in cluster
		INode inode = null;
		BigInteger prevNodeHash = nodesList.get((nodesList.size()+respNodeIndex-1)%nodesList.size()).NodeHash;
		boolean status = false;
		try {
			System.out.println("Calling the node "+nodesList.get(respNodeIndex).Ip+":"+nodesList.get(respNodeIndex).Port+" to move files");
			inode=(INode)Naming.lookup("rmi://"+nodesList.get(respNodeIndex).Ip + ":" + nodesList.get(respNodeIndex).Port + "/nodeServer");
			status = inode.addNodeHandler(newNodeIP, newNodePort, newNodeHash, prevNodeHash);
			
			
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
		return status;
	}


	@Override
	public String put(byte[] input) {
		INode inode=null;
		PutResponse prbResp=null;
		
		int ClusterSize=nodesList.size();
		
		if(ClusterSize<N){
			return "Failure : The no. of nodes in the cluster are < N . Hence can't serve request";
		}
		if(CurrentIndex>=ClusterSize){
			CurrentIndex=0;
		}
		NodeInfo ipNport= nodesList.get(CurrentIndex++);
		try {
			inode=(INode)Naming.lookup("rmi://"+ipNport.Ip+":" +ipNport.Port +"/nodeServer");
				 prbResp= PutResponse.parseFrom(inode.put(input));
		} catch (ConnectException e) {
			// Request the next node if the current node is not available
			System.out.println("Unable to send request for Put");
			put( input);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
			return "Failure";
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		
		return prbResp.getMsg();
	}
	
	@Override
	public byte[] get(String Key) {
		int ClusterSize=nodesList.size();
		if(CurrentIndex>=ClusterSize){
			CurrentIndex=0;
		}
		
		NodeInfo ipNport= nodesList.get(CurrentIndex++);
		INode inode=null;
		try {
			inode=(INode)Naming.lookup("rmi://"+ipNport.Ip+":"+ipNport.Port+"/nodeServer");
		} catch (MalformedURLException | RemoteException | NotBoundException e1) {
			e1.printStackTrace();
		}

		GetRequest.Builder grb = GetRequest.newBuilder();
		grb.setKey(Key);
		grb.setIsReqFromLB(true);
		
		GetResponse gr =null;
		try {
			byte[] resp =inode.get(grb.build().toByteArray());
			if(resp!=null){
				gr = GetResponse.parseFrom(resp);
			}else
			{	System.out.println("response is null");
				GetResponse.Builder getRespBuilder = GetResponse.newBuilder();
				getRespBuilder.setStatus(false);
				return getRespBuilder.build().toByteArray();
			}
		}catch (ConnectException e) {
			System.out.println("Unable to send request for get");
			get(Key);
		} catch (InvalidProtocolBufferException | RemoteException e) {
			e.printStackTrace();
		}
		return gr.toByteArray();
	}

	@Override
	public boolean removeNode(Integer NodeId) throws RemoteException {
		System.out.println("Request received to remove node "+NodeId);
		if(nodeIdList.contains(NodeId)){
			nodeIdList.remove(NodeId);
			for(int i=0;i<nodesList.size();i++){
				if(nodesList.get(i).NodeId == NodeId){
						deletedNodesMap.put(nodesList.get(i).Ip+nodesList.get(i).Port, nodesList.get(i).NodeId);
						if(nodesList.remove(i)!=null){
						System.out.println("Node removed successfully..");
						for(int j=0;j<nodesList.size();j++){
							System.out.println(""+nodesList.get(j).NodeId);
						}
						return true;
						}
				}
			}
		}
		System.out.println("Node Id already removed hence not removing");
		return true;
	}
}

/*package gossip;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;

import node.INode;
import node.MembershipInfoNode;
import node.NodeImpl;
import node.PreferenceListNode;
import protobuf.DynamoProto.MembershipDetails;
import protobuf.DynamoProto.SendGossipRequest;

public class Gossip {
	
	public void sendGossip() throws RemoteException {
		System.out.println("Gossipping Started");
		Runnable runnable = new Runnable() {
			public void run() {
				// task to run goes here
				doSendGossip();
				// System.out.println("heart beat
				// report"+System.currentTimeMillis());
			}
		};
		System.out.println("TGossip is.............................."+NodeImpl.TGossip);
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		service.scheduleAtFixedRate(runnable, 0, NodeImpl.TGossip, TimeUnit.SECONDS);

	}
	

	private void doSendGossip() {
		System.out.println("doSendGossip() invoked");
		INode node;
		SendGossipRequest.Builder sgr = SendGossipRequest.newBuilder();
		MembershipDetails.Builder mbr = MembershipDetails.newBuilder();
		
		System.out.println("mem details size..."+NodeImpl.membershipDetails.size());
		if(NodeImpl.membershipDetails.size()<=1){
			return;
		}

		Map<Integer, MembershipInfoNode> map = NodeImpl.membershipDetails;
		System.out.println("Incrementing the counter for the node "+NodeImpl.myNodeId);
		System.out.println(NodeImpl.membershipDetails);
		map.get(NodeImpl.myNodeId).setCounter(map.get(NodeImpl.myNodeId).getCounter() + 1);
		System.out.println("Incremented value is "+map.get(NodeImpl.myNodeId).getCounter());
		map.get(NodeImpl.myNodeId).setSysTimestamp(System.currentTimeMillis());
		
		for (Map.Entry<Integer, MembershipInfoNode> entry : NodeImpl.membershipDetails.entrySet()) {
			//System.out.println("--- "+entry.getKey());
			if (entry.getValue().isPossiblyFailed())
				continue;
			mbr.setNodeId(entry.getKey());
			mbr.setCounter(entry.getValue().getCounter());
			mbr.setIp(entry.getValue().getIp());
			mbr.setNodeHash(NodeImpl.myNodeHash.toString());
			mbr.setPort(NodeImpl.myPort);
			mbr.setSysTimestamp(entry.getValue().getSysTimestamp());

			sgr.addMemDetails(mbr);
		}

		String RandomIp = getRandomIp();
		System.out.println("Sending gossip to node " + RandomIp);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		int PortNum = 1099;

		try {
			node = (INode) Naming.lookup("rmi://" + RandomIp + ":" + PortNum + "/nodeServer");
			node.hearGossip(sgr.build().toByteArray());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
		System.out.println("returning from dosendgossip");
	}

	private String getRandomIp() {
		System.out.println("Getting random ip");
		Random generator = new Random();
		Object[] values = NodeImpl.membershipDetails.values().toArray();
		while (true) {

			Object random = values[generator.nextInt(values.length)];
			MembershipInfoNode randomNode = (MembershipInfoNode) random;
			int randomNodeId = randomNode.getNodeId();
			if (randomNodeId == NodeImpl.myNodeId)
				continue;
			else {
				System.out.println("Random node selected as " + randomNodeId);
				return NodeImpl.membershipDetails.get(randomNodeId).getIp();
			}
		}
	}

// ------------------------------------------------------------------------------------------------------------------------//
		
	public void hearGossip(byte[] inp) {
		try {
			System.out.println("---------------------------------------------");
			System.out.println("Hear gossip invoked .....");
			SendGossipRequest sgrStream = SendGossipRequest.parseFrom(inp);

			// Loop through what you got
			// Change the implementation to an effective way so that nodes are
			// not iterated through again
			for (MembershipDetails md : sgrStream.getMemDetailsList()) {
				int NodeId = md.getNodeId();
				BigInteger NodeHash= new BigInteger( md.getNodeHash());
				int Port=md.getPort();
				int Counter = md.getCounter();
				String Ip = md.getIp();
				long NodeTimeStamp = md.getSysTimestamp();
				// if(!membershipDetails.containsKey(NodeId))continue;
				System.out.println(" processing for node ....." + NodeId);
				if (NodeImpl.membershipDetails.containsKey(NodeId)) {
					System.out.println("Counter received is "+Counter);
					System.out.println("Counter present  is "+NodeImpl.membershipDetails.get(NodeId).getCounter());
					if (Counter <= NodeImpl.membershipDetails.get(NodeId).getCounter())
						continue;

					int TimeDiff = (int) (System.currentTimeMillis() - NodeTimeStamp) / 1000;
					if (NodeImpl.membershipDetails.get(NodeId).isPossiblyFailed()) {
						if (TimeDiff >= NodeImpl.TCleanUp) {
							NodeImpl.membershipDetails.remove(NodeId);
							NodeImpl.preferenceList.deleteNode(NodeId);
							System.out.println("Cleanup done for node " + NodeId);
							// Since this is a fail node we need to skip the
							// addition to membershipdetails
							continue;
						}
						// the failed node has come up again
						else if (TimeDiff < NodeImpl.TFail) {
							NodeImpl.membershipDetails.get(NodeId).setPossiblyFailed(false);
						} else { // the timediff is less than TCleanup and
									// greater than Tfail.. dont do anything
							continue;
						}

					} else if (TimeDiff >= NodeImpl.TFail) {
						// There is a possible failure of the node
						// Need to wait for TCleanup and clean it
						System.out.println("Adding the node " + NodeId + " to possible fail nodes list");
						NodeImpl.membershipDetails.get(NodeId).setPossiblyFailed(true);
						NodeImpl.preferenceList.deleteNode(NodeId);
						continue;

					}
				}
				System.out.println("counter received for " + NodeId + " is " + Counter + ". existing counter ");

				MembershipInfoNode nd = new MembershipInfoNode(NodeId, NodeHash,Port,Counter, Ip, NodeTimeStamp, false);
				NodeImpl.membershipDetails.put(NodeId, nd);
				System.out.println("size of mem...after put.."+NodeImpl.membershipDetails.size());
				//TODO: if  node comes up with different hash, it will not get updated in pref list
				System.out.println("################################################################");
				System.out.println("adding the node "+ NodeId +" to the preference list");
				NodeImpl.preferenceList.addNode(NodeId,NodeHash);
			}
			// ------------------------------------------------------------------------------------
			// Loop through what you have
			for (Map.Entry<Integer, MembershipInfoNode> entry : NodeImpl.membershipDetails.entrySet()) {
				int TimeDiff = (int) (System.currentTimeMillis() - entry.getValue().getSysTimestamp()) / 1000;
				int NodeId = entry.getValue().getNodeId();
				if (TimeDiff >= NodeImpl.TCleanUp) {
					System.out.println("Removing the node id "+NodeId+" as the time diff is > TCleanup");
					NodeImpl.membershipDetails.remove(NodeId);
				} else if (TimeDiff >= NodeImpl.TFail) {
					// There is a possible failure of the node
					// Need to wait for TCleanup and clean it
					System.out.println("Adding the node " + NodeId + " to possible fail nodes list");
					NodeImpl.membershipDetails.get(NodeId).setPossiblyFailed(true);
					NodeImpl.preferenceList.deleteNode(entry.getValue().getNodeId());
				}
			}

			System.out.println("-------------------------");
			System.out.println("The new membership details are");
			for (Entry<Integer, MembershipInfoNode> entry : NodeImpl.membershipDetails.entrySet()) {
				if (entry.getValue().isPossiblyFailed()){
					System.out.println("following node is marked as possibly failed");
				}
			
				System.out.println("node id " + entry.getKey());
				System.out.println("counter  " + entry.getValue().getCounter());
				System.out.println("Timestamp " + entry.getValue().getSysTimestamp());
			}
			
			System.out.println("-------------------------");
			System.out.println("The new preference details are");
			for ( PreferenceListNode N: NodeImpl.preferenceList.nodesList) {
				
				System.out.println("node id " + N.getNodeId());
				System.out.println("node #  " + N.getNodeHash());
			}			
			// int nodeId= sgrStream.getMemDetails(0).getNodeId();
			// System.out.println("Gossip received from node "+nodeId);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTra`1ce();
		}
		System.out.println("hear gossip done ......");

	}
}
*/
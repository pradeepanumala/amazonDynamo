package node;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.google.protobuf.InvalidProtocolBufferException;

import loadBalancer.ILoadBalancer;
import protobuf.DynamoProto.MembershipDetails;
import protobuf.DynamoProto.PutRequest;
import protobuf.DynamoProto.SendGossipRequest;
import protobuf.LoadBalancerProto.ClusterMembers;
import protobuf.LoadBalancerProto.Config;
import protobuf.LoadBalancerProto.NodeDetails;
import utility.Utility;

@SuppressWarnings("serial")
public class NodeImpl extends UnicastRemoteObject implements INode {
	// All these values are default. Actual values are fetched from the loadbalancer when the node comes up initially
	static String myIp = "10.0.0.1";
	static int myPort;
	static BigInteger myNodeHash;
	static int myNodeId;
	static String configFilesPath = "";
	static String membershipFile = "";
	static int TGossip;
	static int R;
	static int W;
	static int N;
	static int LBInformerCount;
	static int TFail; // If no heartbeat until TFail then node failure
	static int TCleanUp;
	static PreferenceList preferenceList;
	public static boolean debug = false;
	static DB db;
	static ClusterMembers cm;
	static int fileCounter = 100000;
	static String dbPath;
	static Operations operations;
	// Map between node id and other node details
	static HashMap<Integer, MembershipInfoNode> membershipDetails = new HashMap<Integer, MembershipInfoNode>();
	static HashSet<String> filesMoved = null;
	static BigInteger lowerCurentlyMovingKey = null;
	static BigInteger upperCurentlyMovingKey = null;
	static ArrayList<BigInteger> lowerMovedKeyRange = null;
	static ArrayList<BigInteger> upperMovedKeyRange = null;
	static HashSet<Integer> nodesDeletedInLB = new HashSet<Integer>();
	static boolean isMovingKeyRange = false;
	static BigInteger newNodeHash;
	static String newNodeIp;
	static int newNodePort;
	static final String LoadBalancerIp="10.0.0.1";
	static final int LoadBalancerPort=1099;
	Utility util;

	public NodeImpl() throws RemoteException {
		super();

		try {
			Options options = new Options();
			options.createIfMissing(true);
			db = factory.open(new File(dbPath), options);

			util = new Utility();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		NodeImpl node;

		try {

			if (args.length >= 2) {
				myIp = args[0];
				myPort = Integer.parseInt(args[1]);
			} else if (args.length == 1) {
				myIp = args[0];
			} else {
				System.err.println("Please provide the path for configuration file excluding filename");
				System.exit(1);
			}
			new File("/opt/data/instance_" + myPort).mkdir();
			dbPath = "/opt/data/instance_" + myPort + "/myDb1";
			node = new NodeImpl();
			Registry registry = null;
			try {
				registry = LocateRegistry.getRegistry(myPort);
				registry.list();
			} catch (RemoteException e) {
				registry = LocateRegistry.createRegistry(myPort);
			}
			Naming.rebind("rmi://" + myIp + ":" + myPort + "/nodeServer", node);

			addToCluster();
			operations = new Operations();
			System.out.println("Node Server Started...");

			node.sendGossip();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addToCluster() {
		ILoadBalancer iLoadBalancer;
		try {
			db.close();
			iLoadBalancer = (ILoadBalancer) Naming.lookup("rmi://"+LoadBalancerIp+":"+LoadBalancerPort+"/loadBalancer");

			cm = cm.parseFrom(iLoadBalancer.addNode(myIp, myPort));
			if (!cm.getStatus()) {
				System.err.println("Failed to add node to the cluster");
				System.exit(1);
			}
			myNodeId = cm.getMyNodeId();
			myNodeHash = new BigInteger(cm.getMyNodeHash().getValue().toByteArray());

			// get the config details
			Config config = cm.getConfigDetails();
			N = config.getN();
			R = config.getR();
			W = config.getW();
			LBInformerCount = config.getLBInformerCount();
			TFail = config.getTFail();
			TGossip = config.getTGossip();
			TCleanUp = config.getTCleanUp();
			db = factory.open(new File(dbPath), new Options());
			preferenceList = new PreferenceList(N);

			for (NodeDetails nd : cm.getClusterMemsList()) {
				Utility.trace("node hash " + nd.getNodeHash());
				MembershipInfoNode mem = new MembershipInfoNode(nd.getNodeId(),
						new BigInteger(nd.getNodeHash().getValue().toByteArray()), nd.getPort(), 0, nd.getIp(), 0,
						false);
				membershipDetails.put(nd.getNodeId(), mem);
				nodesDeletedInLB.remove(nd.getNodeId());
			}

			Utility.trace("membership details received from loadbalancer ");
			for (Entry<Integer, MembershipInfoNode> entry : membershipDetails.entrySet()) {
				
				Utility.trace("Node Id   : " +entry.getValue().getNodeId());
				Utility.trace("Node Port : " +entry.getValue().getPort());
				Utility.trace("Node Hash : " +entry.getValue().getNodeHash());
				
				// Add the membership details to preference list
				preferenceList.addNode(entry.getValue().getNodeId(), entry.getValue().getNodeHash());
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	// @Override
	public byte[] get(byte[] inp) throws RemoteException {
		return operations.get(inp);
	}

	public byte[] put(byte[] inp) throws RemoteException {
		return operations.put(inp);
	}

	public void hearGossip(byte[] inp) {
		try {
			List<Integer> failedNodes = new ArrayList<Integer>();
			HashMap<Integer, MembershipInfoNode> membershipDetailsCopy = new HashMap<Integer, MembershipInfoNode>(membershipDetails);
			SendGossipRequest sgrStream = SendGossipRequest.parseFrom(inp);

			// Loop through what you got from other node
			// Change the implementation to an effective way so that nodes are
			// not iterated through again
			for (MembershipDetails md : sgrStream.getMemDetailsList()) {
				int NodeId = md.getNodeId();
				BigInteger NodeHash = new BigInteger(md.getNodeHash());
				int Port = md.getPort();
				int Counter = md.getCounter();
				String Ip = md.getIp();
				long NodeTimeStamp = md.getSysTimestamp();
				membershipDetailsCopy.remove(NodeId);

				MembershipInfoNode nd = new MembershipInfoNode(NodeId, NodeHash, Port, Counter, Ip, NodeTimeStamp,false);
				if (membershipDetails.containsKey(NodeId)) {
					if (Counter <= membershipDetails.get(NodeId).getCounter())
						continue;

					int TimeDiff = (int) (System.currentTimeMillis() - NodeTimeStamp) / 1000;
					if (membershipDetails.get(NodeId).isPossiblyFailed()) {
						System.out.println("Node Id " + NodeId + "is found in possibly failed list");
						if (TimeDiff >= TCleanUp) {
							failedNodes.add(NodeId);
							System.out.println("Cleanup done for node " + NodeId);
						}
						// the failed node has come up again
						else if (TimeDiff < TFail) {
							membershipDetails.get(NodeId).setPossiblyFailed(false);
							preferenceList.addNode(NodeId, NodeHash);
						}
					} else if (TimeDiff >= TFail && !membershipDetails.get(NodeId).isPossiblyFailed()) {
						// There is a possible failure of the node. Set possibly
						// failed to true and delete it from pref list
						// Need to wait for TCleanup and clean it
						Utility.trace("Adding the node " + NodeId + " to possible fail nodes list as TimeDiff is :"	+ TimeDiff);
						membershipDetails.get(NodeId).setPossiblyFailed(true);
						// Dont delete from pref list. we are marking as failed
						// in membershiplist
						// preferenceList.deleteNode(NodeId);
						// continue;

					} else {
						membershipDetails.put(NodeId, nd);
						nodesDeletedInLB.remove(NodeId);
					}
				} else {
					membershipDetails.put(NodeId, nd);
					preferenceList.addNode(NodeId, NodeHash);
					nodesDeletedInLB.remove(NodeId);
				}
			}

			for (int k = 0; k < failedNodes.size(); k++) {
				// Don't delete from any of the list. Just mark failed in membership details
				membershipDetails.get(failedNodes.get(k)).setIsFailed(true);
				if (!nodesDeletedInLB.contains(failedNodes.get(k))) {
					removeNodeFromLB(new Integer(failedNodes.get(k)));
				}
			}
			
			failedNodes = new ArrayList<Integer>();

			// ------------------------------------------------------------------------------------
			// Loop through what you have
			for (Map.Entry<Integer, MembershipInfoNode> entry : membershipDetails.entrySet()) {

				int TimeDiff = (int) (System.currentTimeMillis() - entry.getValue().getSysTimestamp()) / 1000;
				if (entry.getKey() == myNodeId || (entry.getValue().getCounter() == 0) || entry.getValue().isIsFailed())
					continue;
				int NodeId = entry.getValue().getNodeId();
				boolean isPossiblyFailed = membershipDetails.get(NodeId).isPossiblyFailed();

				// When a node is started for the first time, it gets the 0 timestamp
				// from the loadbalancer making the TimeDiff = currentTimestamp. 
				// Hence added the condition isPossiblyFailed to avoid false positive
				
				if (TimeDiff >= TCleanUp) {
					Utility.trace("TimeDiff is " + TimeDiff + " seconds for node " + entry.getKey()+ "..hence removing node " + NodeId);
					failedNodes.add(NodeId);
				} else if (TimeDiff >= TFail && !isPossiblyFailed) {
					// There is a possible failure of the node
					// Need to wait for TCleanup and clean it
					Utility.trace("The timediff is " + TimeDiff);
					membershipDetails.get(NodeId).setPossiblyFailed(true);
				}
			}

			for (int k = 0; k < failedNodes.size(); k++) {
				// Dont delete from any of the list. Just mark failed in membership details
				membershipDetails.get(failedNodes.get(k)).setIsFailed(true);
				if (!nodesDeletedInLB.contains(failedNodes.get(k))) {
					removeNodeFromLB(new Integer(failedNodes.get(k)));
				}
			}

		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}

	}

	public void sendGossip() throws RemoteException {
		System.out.println("Gossipping Started");
		Runnable runnable = new Runnable() {
			public void run() {
				// task to run goes here
				doSendGossip();
			}
		};

		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		service.scheduleAtFixedRate(runnable, 0, TGossip, TimeUnit.SECONDS);

	}

	private void doSendGossip() {
		if (membershipDetails.size() <= 1) {
			return;
		}
		INode node;
		SendGossipRequest.Builder sgr = SendGossipRequest.newBuilder();
		MembershipDetails.Builder mbr = MembershipDetails.newBuilder();

		Map<Integer, MembershipInfoNode> map = membershipDetails;
		map.get(myNodeId).setCounter(map.get(myNodeId).getCounter() + 1);
		map.get(myNodeId).setSysTimestamp(System.currentTimeMillis());
		for (Map.Entry<Integer, MembershipInfoNode> entry : map.entrySet()) {
			if (entry.getValue().isPossiblyFailed() || entry.getValue().isIsFailed())
				continue;
			mbr.setNodeId(entry.getKey());
			mbr.setCounter(entry.getValue().getCounter());
			mbr.setIp(entry.getValue().getIp());
			mbr.setNodeHash(entry.getValue().getNodeHash().toString());
			mbr.setPort(entry.getValue().getPort());
			mbr.setSysTimestamp(entry.getValue().getSysTimestamp());

			sgr.addMemDetails(mbr);
		}

		int RandomNodeId = getRandomNode();
		String RandomIp = membershipDetails.get(RandomNodeId).getIp();
		int RandomPort = membershipDetails.get(RandomNodeId).getPort();
	
		try {
			node = (INode) Naming.lookup("rmi://" + RandomIp + ":" + RandomPort + "/nodeServer");
			node.hearGossip(sgr.build().toByteArray());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			System.out.println("Unable to send gossip to " + RandomPort + ".. The node may be down");
		}
	}

	private int getRandomNode() {
		Random generator = new Random();

		Object[] values = preferenceList.nodesList.toArray();
		
		while (true) {
			Object random = values[generator.nextInt(values.length)];
			PreferenceListNode randomNode = (PreferenceListNode) random;
			int randomNodeId = randomNode.getNodeId();
			if (randomNodeId == myNodeId || membershipDetails.get(randomNodeId).isPossiblyFailed()
					|| membershipDetails.get(randomNodeId).isIsFailed())
				continue;
			else {
				return randomNodeId;
			}
		}
	}

	private void removeNodeFromLB(int NodeId) {
		ILoadBalancer iLoadBalancer;
		try {
			iLoadBalancer = (ILoadBalancer) Naming.lookup("rmi://"+LoadBalancerIp+":"+LoadBalancerPort+"/loadBalancer");
			iLoadBalancer.removeNode(NodeId);
			informOtherNodes(NodeId);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
	}

	private void informOtherNodes(Integer NodeId) {
		INode inode;
		try {
			for (int i = 0; i < LBInformerCount; i++) {
				int randomNodeId = getRandomNode();
				String RandomIp = membershipDetails.get(randomNodeId).getIp();
				int RandomPort = membershipDetails.get(randomNodeId).getPort();
				Utility.trace("Informing the node " + randomNodeId + "that LB is already updated about node failure");
				inode = (INode) Naming.lookup("rmi://" + RandomIp + ":" + RandomPort + "/nodeServer");
				inode.nodeDelInLB(NodeId);
			}
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean addNodeHandler(String newNodeIP, int newNodePort, BigInteger newNodeHash, BigInteger prevNodeHash) {
		isMovingKeyRange = true;
		// repairDb();
		NodeImpl.newNodeHash = newNodeHash;
		NodeImpl.newNodeIp = newNodeIP;
		NodeImpl.newNodePort = newNodePort;
		filesMoved = new HashSet<String>();
		lowerMovedKeyRange = new ArrayList<BigInteger>();
		upperMovedKeyRange = new ArrayList<BigInteger>();

		handleFileMovement(newNodeIP, newNodePort, newNodeHash, prevNodeHash);

		isMovingKeyRange = false;
		filesMoved = null;
		lowerMovedKeyRange = null;
		upperMovedKeyRange = null;

		return true;
	}

	private boolean handleFileMovement(String newNodeIP, int newNodePort, BigInteger newNodeHash,
			BigInteger prevNodeHash) {
		boolean isReFetchReqd = false;
		INode node = null;
		try {

			node = (INode) Naming.lookup("rmi://" + newNodeIP + ":" + newNodePort + "/nodeServer");
			ArrayList<String> logFileNames = util.getFileNames(dbPath, ".log");
			ArrayList<FileInfo> fileInfoList = getSSTFileInfo(prevNodeHash, newNodeHash);
			Utility.trace("logFileNames.." + logFileNames.size() + ".");
			Utility.trace(logFileNames.get(0));
			if (!filesMoved.contains(logFileNames.get(0))) {
				File file = new File(dbPath + "/" + logFileNames.get(0));
				if (!file.exists()) {
					isReFetchReqd = true;
				} else {
					// Thread.sleep(60000);
					boolean isFileMoved = moveFile(logFileNames.get(0), dbPath, node);
					if (isFileMoved) {
						node.repairDb();
						filesMoved.add(logFileNames.get(0));
					}
				}
			}
			for (int i = 0; i < fileInfoList.size(); i++) {
			//	Thread.sleep(60000);
				Utility.trace("moving file " + fileInfoList.get(i).fileName);
				
				if (filesMoved.contains(fileInfoList.get(i).fileName)) {
					Utility.trace("File is already moved. Hence not moving now");
					continue;
				}
				File file = new File(dbPath + "/" + fileInfoList.get(i).fileName);
				if (!file.exists()) {
					isReFetchReqd = true;
				} else {
					lowerCurentlyMovingKey = fileInfoList.get(i).lower;
					upperCurentlyMovingKey = fileInfoList.get(i).upper;
					Utility.trace("lowerCurentlyMovingKey : " + lowerCurentlyMovingKey);
					Utility.trace("upperCurentlyMovingKey : " + upperCurentlyMovingKey);
					Utility.trace("File movement is in progress....");
					// Thread.sleep(60000);
					boolean isFileMoved = moveFile(fileInfoList.get(i).fileName, dbPath, node);
					if (isFileMoved) {
						lowerCurentlyMovingKey = null;
						upperCurentlyMovingKey = null;
						node.repairDb();
						filesMoved.add(fileInfoList.get(i).fileName);
						lowerMovedKeyRange.add(fileInfoList.get(i).lower);
						upperMovedKeyRange.add(fileInfoList.get(i).upper);
					}
					//Thread.sleep(60000);
				}
			}

			if (isReFetchReqd) {
				return addNodeHandler(newNodeIP, newNodePort, newNodeHash, prevNodeHash);
			}
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
			return false;
		}

		return false;

	}

	private boolean moveFile(String fileName, String path, INode node) {
		try {
			File file = new File(path + "/" + fileName);
			// Defines buffer in which the file will be read
			byte buffer[] = new byte[(int) file.length()];
			BufferedInputStream inputFileStream = new BufferedInputStream(new FileInputStream(path + "/" + fileName));
			// Reads the file into buffer
			inputFileStream.read(buffer, 0, buffer.length);
			inputFileStream.close();

			String splitParts[] = fileName.split("\\.");
			node.putFile(buffer, splitParts[1]);
		} catch (Exception e) {
			System.out.println("FileImpl:" + e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	private ArrayList<FileInfo> getSSTFileInfo(BigInteger start, BigInteger end) {
		ArrayList<FileInfo> fileInfoList = new ArrayList<FileInfo>();

		String output = db.getProperty("leveldb.sstables");

		String splioutput[] = output.split("\n");

		int i = 0;

		for (i = 0; i < splioutput.length; i++) {
			String line = splioutput[i];
			if (line.startsWith("--")) {
				continue;
			}
			String lineSplit[] = line.split("'");

			BigInteger lower = new BigInteger(lineSplit[1]);
			BigInteger upper = new BigInteger(lineSplit[3]);
			boolean isEligibleForMove = false;
			if (start.compareTo(lower) >= 0 && start.compareTo(upper) <= 0) {
				isEligibleForMove = true;
			} else if (end.compareTo(lower) >= 0 && end.compareTo(upper) <= 0) {
				isEligibleForMove = true;
			} else if (start.compareTo(lower) <= 0 && end.compareTo(upper) >= 0) {
				isEligibleForMove = true;
			} else {
				Utility.trace("File " + lineSplit[0].split(":")[0] + ".sst is not eligible");
			}

			if (isEligibleForMove) {
				String fileName = lineSplit[0].split(":")[0];
				fileName = String.format("%06d", Integer.parseInt(fileName.trim()));
				fileInfoList.add(new FileInfo(fileName + ".sst", lower, upper));
			}

		}

		return fileInfoList;
	}

	@Override
	public void putFile(byte[] inp, String extn) throws RemoteException {
		fileCounter++;
		File file = new File(dbPath + "/" + fileCounter + "." + extn);
		try {
			BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(file.getAbsolutePath()));
			outputFile.write(inp, 0, inp.length);
			outputFile.flush();
			outputFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void repairDb() {
		Options options = new Options();
		try {
			factory.repair(new File(dbPath), options);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nodeDelInLB(Integer NodeId) throws RemoteException {
		if (!nodesDeletedInLB.contains(NodeId)) {
			nodesDeletedInLB.add(NodeId);
			Utility.trace("Informing other nodes not to inform LB.. not invoking LB");
			informOtherNodes(NodeId);
		} else {
			Utility.trace("I am already informed about the nodefailure");
		}
	}

	public boolean hintedHandoff(String newNodeIP, int newNodePort, BigInteger newNodeHash, int newNodeID,
			boolean fromLB) throws RemoteException {

		try {
			INode node;
			if (fromLB) {
				List<Integer> topNList = (List<Integer>) NodeImpl.preferenceList.getTopN(newNodeHash);
				int i;

				for (i = 0; i < topNList.size(); i++) {
					if (membershipDetails.get(topNList.get(i)).isIsFailed()) {
						System.out.println("Failing to call hinted handoff from "
								+ membershipDetails.get(topNList.get(i)).getPort() + "to " + newNodePort);
						continue;
					}
					System.out.println("Calling to send hinted handoff data from "
							+ membershipDetails.get(topNList.get(i)).getPort() + "to " + newNodePort);
					node = (INode) Naming.lookup("rmi://" + membershipDetails.get(topNList.get(i)).getIp() + ":"
							+ membershipDetails.get(topNList.get(i)).getPort() + "/nodeServer");
					node.hintedHandoff(newNodeIP, newNodePort, newNodeHash, newNodeID, false);
				}
				//call hinted handoff from the node to itself
				node = (INode) Naming.lookup("rmi://" + newNodeIP + ":"
						+ newNodePort + "/nodeServer");
				node.hintedHandoff(newNodeIP, newNodePort, newNodeHash, newNodeID, false);

			} else {
				if (Operations.dbInstancesMap.containsKey(newNodeID)) {
					node = (INode) Naming.lookup("rmi://" + newNodeIP + ":" + newNodePort + "/nodeServer");
					String path = "/opt/data/instance_" + myPort + "/DB_" + newNodeID;
					File f = new File(path);
					if (f.exists() && f.isDirectory()) {
						System.out.println("I " + myPort + "am handling hinted handoff data from to " + newNodePort);
						Operations.dbInstancesMap.get(newNodeID).close();
						Operations.dbInstancesMap.remove(newNodeID);
						ArrayList<String> logFileNames = util.getFileNames(path, ".log");
						ArrayList<String> sstFileNames = util.getFileNames(path, ".sst");

						logFileNames.addAll(sstFileNames);
						for (int i = 0; i < logFileNames.size(); i++) {
							boolean isFileMoved = moveFile(logFileNames.get(i), path, node);
						}
						node.repairDb();
					}
					util.deleteDirectory(new File(path));
				} else {
					Utility.trace("I" + myPort + " don't have data for " + newNodePort);
				}
			}
		} catch (MalformedURLException | NotBoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public byte[] putDuringKeyMov(byte[] inp, int coordinatorId) throws RemoteException {
		try {

			Operations op = new Operations();
			PutRequest.Builder prb;
			prb = PutRequest.newBuilder(PutRequest.parseFrom(inp));

			prb.setIsReqFromLB(false);
			prb.setIsReqFromMediator(false); 
			prb.setIsReqFromCoordinator(true);
			prb.setCoordinatorId(coordinatorId);
			prb.setCoordinatorTimestamp(System.currentTimeMillis());
			prb.setReplacedNodeId(myNodeId);
			return op.put(prb.build().toByteArray());
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return null;
	}
}
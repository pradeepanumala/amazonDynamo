package node;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.google.protobuf.InvalidProtocolBufferException;

import protobuf.DynamoProto.ConcValue;
import protobuf.DynamoProto.GetRequest;
import protobuf.DynamoProto.GetResponse;
import protobuf.DynamoProto.GetResult;
import protobuf.DynamoProto.PutRequest;
import protobuf.DynamoProto.PutResponse;
import protobuf.DynamoProto.Vector;
import protobuf.DynamoProto.VersionValue;
import utility.Utility;
import utility.VectorComparator;

// The logic for the basic put and get operations go here
public class Operations {
	boolean debug = NodeImpl.debug;
	Utility utility_;
	int N = NodeImpl.N;
	int R = NodeImpl.R;
	int W = NodeImpl.W;
	DB db = NodeImpl.db;
	String myIp = NodeImpl.myIp;
	int myPort = NodeImpl.myPort;
	int myNodeId = NodeImpl.myNodeId;
	List<byte[]> resultStream;
	ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
	static ConcurrentMap<Integer, DB> dbInstancesMap = new ConcurrentHashMap<Integer, DB>();

	public Operations() {
		utility_ = new Utility();
	}

	public byte[] get(byte[] inp) throws RemoteException {
		String TableName = "";
		boolean IsReqFromLB;
		int replacedNodeId;
		resultStream = Collections.synchronizedList(new ArrayList<byte[]>());
		try {
			GetRequest grStream = GetRequest.parseFrom(inp);
			String Key = grStream.getKey();
			TableName = grStream.getTableName();
			IsReqFromLB = grStream.getIsReqFromLB();
			replacedNodeId = grStream.getReplacedNodeId();
			if (IsReqFromLB) {
				byte[] res = getForLB(Key, TableName);
				return res;
			} else {
				return getForCoordinator(Key, TableName, replacedNodeId);
			}

		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	private byte[] getForLB(final String Key, String TableName) throws InterruptedException {

		int i;
		int lastNodeId = -1;
		final AtomicInteger doneCount = new AtomicInteger(0);
		final AtomicInteger failedCount = new AtomicInteger(0);
	//	final ConcurrentMap<Integer, String> resultMap = new ConcurrentHashMap<Integer, String>();
		Utility.trace("getForLB : Key is : " + Key.toString());
		BigInteger KeyHash = utility_.md5(Key);
		final List<Integer> topNList = (List<Integer>) NodeImpl.preferenceList.getTopN(KeyHash);
		lastNodeId = topNList.get(topNList.size() - 1);
		int replacedNodeId = -1;
		for (i = 0; i < N; i++) {
			replacedNodeId = -1;

			int NodeId = topNList.get(i);

			// Check if this node isn't a failed node.
			// If failed node, then get (N+ i) node and get the work done.
			if (NodeImpl.membershipDetails.get(NodeId).isPossiblyFailed()
					|| NodeImpl.membershipDetails.get(NodeId).isIsFailed()) {
				replacedNodeId = NodeId;
				while (NodeId != -1 && (NodeImpl.membershipDetails.get(NodeId).isPossiblyFailed()
						|| NodeImpl.membershipDetails.get(NodeId).isIsFailed())) {
					NodeId = NodeImpl.preferenceList.getNextAvailableNode(topNList.get(0), lastNodeId);
					lastNodeId = NodeId;
				}

				if (NodeId == -1) {
					// No more running nodes left. Error out.
					break;

				}
			}
			final String ip = NodeImpl.membershipDetails.get(NodeId).getIp();
			final int port = NodeImpl.membershipDetails.get(NodeId).getPort();
			final int finalReplacedNodeId = replacedNodeId;
			Thread t = new Thread(new Runnable() {
				public void run() {
					INode inode;
					try {

						inode = (INode) Naming.lookup("rmi://" + ip + ":" + port + "/nodeServer");
						GetRequest.Builder grb = GetRequest.newBuilder();
						grb.setKey(Key);
						grb.setIsReqFromLB(false);
						grb.setReplacedNodeId(finalReplacedNodeId);
						byte[] output = inode.get(grb.build().toByteArray());

						if (output != null) {
							resultStream.add(output);
							doneCount.incrementAndGet();
						} else {
							failedCount.incrementAndGet();
						}
					} catch (MalformedURLException e) {
						e.printStackTrace();
					} catch (RemoteException e) {
						e.printStackTrace();
					} catch (NotBoundException e) {
						e.printStackTrace();
					}

				}
			});
			t.start();

		}

		while (true) {
			if (doneCount.get() >= R) {
				break;
			} else if ((failedCount.get() + doneCount.get() == N) && (doneCount.get()) < R) {
				Utility.trace("Failed Count is "+failedCount.get());
				return null;
			}
		}
		if (resultStream != null) {
			return reconcile(resultStream);
		}

		return null;
	}

	private byte[] getForCoordinator(String Key, String TableName, int replacedNodeId) {
		DB db1 = null;
		BigInteger KeyHash = utility_.md5(Key);
		String KeyHashString = KeyHash.toString();
		byte[] value = null;
		while (KeyHashString.length() < 39)
			KeyHashString = "0" + KeyHashString;

		if (replacedNodeId != -1) {

			try {
				if (dbInstancesMap.containsKey(replacedNodeId)) {
					db1 = dbInstancesMap.get(replacedNodeId);
				} else {
					db1 = factory.open(new File("/opt/data/instance_" + myPort + "/DB_" + replacedNodeId),
							new Options());

					dbInstancesMap.put(replacedNodeId, db1);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		} else {
			db1 = db;
		}
		
		value = db1.get(bytes(KeyHashString));
		if (value == null) {
			Utility.trace("Value not found in db " + myPort);
			return null;
		}
		try {
			ConcValue cv = ConcValue.parseFrom(value);
			List<VersionValue> curVerValList = cv.getVersionValueList();

			// loop through each version eg v1
			for (int k = 0; k < curVerValList.size(); k++) {
				VersionValue v = curVerValList.get(k);
				List<Vector> curContextList = new ArrayList<Vector>(v.getContextList());
				for (int j = 0; j < curContextList.size(); j++) {
					Utility.trace(""+curContextList.get(j).getNodeId()+"-"+curContextList.get(j).getCounter());
				}

			}

		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}

		GetResponse.Builder getRespBuilder = GetResponse.newBuilder();
		getRespBuilder.setStatus(true);
	//	ConcValue concValue = null;

		return value;
	}

	public byte[] put(byte[] inp) throws RemoteException {
	//	Boolean isDelete;
		Boolean isReqFromLB;
		Boolean isReqFromMediator;
		Boolean isReqFromCoordinator;

		try {
			PutRequest PRStream = PutRequest.parseFrom(inp);

		//	isDelete = PRStream.getIsDelete();
			isReqFromLB = PRStream.getIsReqFromLB();
			isReqFromMediator = PRStream.getIsReqFromMediator();
			isReqFromCoordinator = PRStream.getIsReqFromCoordinator();
			
			
			if (isReqFromLB) {
				return PutReqFromLB(inp);
			} else if (isReqFromMediator) {
				return PutReqFromMediator(inp);
			} else if (isReqFromCoordinator) {
				return PutReqFromCoordinator(inp);
			}
		} catch (InvalidProtocolBufferException | NotBoundException | InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	private byte[] PutReqFromCoordinator(byte[] inp) {
		putToDB(inp);
		PutResponse.Builder putRespBuilder = PutResponse.newBuilder();
		putRespBuilder.setMsg("Success");
		putRespBuilder.setSuccess(true);
		return putRespBuilder.build().toByteArray();
	}

	// co ordintor serving the request
	private byte[] PutReqFromMediator(byte[] inp) throws InterruptedException {

		int i;
		boolean isKeyMoved = false;
		PutRequest PRStream = null;
		String Key = "";
		int lastNodeId;
		int replacedNodeId = -1;
		try {
			PRStream = PutRequest.parseFrom(inp);
			Key = PRStream.getKey();
		} catch (InvalidProtocolBufferException e1) {
			e1.printStackTrace();
		}

		final AtomicInteger doneCount = new AtomicInteger(0);
		BigInteger KeyHash = utility_.md5(Key);
		final PutRequest finalPRStream = PRStream;
		final long CoordinatorTimeStamp = System.currentTimeMillis();
		final List<Integer> topNList = (List<Integer>) NodeImpl.preferenceList.getTopN(KeyHash);
		lastNodeId = topNList.get(topNList.size() - 1);
		// Check if any movement of keys is happening.

		if (NodeImpl.isMovingKeyRange && (KeyHash.compareTo(NodeImpl.newNodeHash) <= 0)) {
			// isNewNodeCoord = checkIfNewNodeIsCoord(KeyHash,);
			if ((NodeImpl.lowerCurentlyMovingKey != null) && (NodeImpl.upperCurentlyMovingKey != null)
					&& (KeyHash.compareTo(NodeImpl.lowerCurentlyMovingKey) >= 0)
					&& (KeyHash.compareTo(NodeImpl.upperCurentlyMovingKey) <= 0)) {
				// Ask client to wait and return
				Utility.trace("The key falls in the current movement range.. Asking client to wait");
				PutResponse.Builder putRespBuilder = PutResponse.newBuilder();
				putRespBuilder.setMsg("Systems Busy.. Please wait for sometime");
				putRespBuilder.setSuccess(false);
				return putRespBuilder.build().toByteArray();
			} else {
				int rangeSize = 0;
				if (NodeImpl.lowerMovedKeyRange != null)
					rangeSize = NodeImpl.lowerMovedKeyRange.size();
				for (int k = 0; k < rangeSize; k++) {
					if (KeyHash.compareTo(NodeImpl.lowerMovedKeyRange.get(k)) >= 0
							&& KeyHash.compareTo(NodeImpl.upperMovedKeyRange.get(k)) <= 0) {
						isKeyMoved = true;

						// -- spawn a new thread that puts the key/value into new node
						Thread t = new Thread(new Runnable() {
							public void run() {
								INode inode;
								try {
									Utility.trace(" Put Req : File is already transferred. So invoking put on "									// the node"
									 + NodeImpl.newNodeIp + NodeImpl.newNodePort);
									
									inode = (INode) Naming.lookup(
											"rmi://" + NodeImpl.newNodeIp + ":" + NodeImpl.newNodePort + "/nodeServer");
									PutRequest.Builder prb = PutRequest.newBuilder(finalPRStream);

									prb.setIsReqFromLB(false);
									prb.setIsReqFromMediator(false);
									prb.setIsReqFromCoordinator(true);
									prb.setCoordinatorId(myNodeId);
									prb.setCoordinatorTimestamp(CoordinatorTimeStamp);

									PutResponse pr = PutResponse
											.parseFrom(inode.putDuringKeyMov(prb.build().toByteArray(), myNodeId));
									if (pr.getSuccess()) {
										doneCount.incrementAndGet();
									}

								} catch (MalformedURLException e) {
									e.printStackTrace();
								} catch (RemoteException e) {
									e.printStackTrace();
								} catch (NotBoundException e) {
									e.printStackTrace();
								} catch (InvalidProtocolBufferException e) {
									e.printStackTrace();
								}

							}
						});
						t.start();


						break;
					}
				}

				/*// The key is there in the file that is not moved yet
				// We will ask the client to wait

				if (!isKeyMoved) {
					//  Ask client to wait
				}*/
			}
		}

		if (isKeyMoved) {
			// remove the last nodeHash
			topNList.remove(topNList.size() - 1);
		}
		// out of the N nodes to be written to , it might happen the first w
		// writes happen on the nodes other than co ordinatinator.
		// this is also a valid case. Hence we spawn a thread for writing to
		// itself (co ordinator) also.

		for (i = 0; i < topNList.size(); i++) {
			int NodeId = topNList.get(i);
			replacedNodeId = -1;
			// Check if this node isn't a failed node.
			// If failed node, then get (N+ i) node and get the work done.
			if (NodeImpl.membershipDetails.get(NodeId).isPossiblyFailed()
					|| NodeImpl.membershipDetails.get(NodeId).isIsFailed()) {
				replacedNodeId = NodeId;
				while (NodeId != -1 && (NodeImpl.membershipDetails.get(NodeId).isPossiblyFailed()
						|| NodeImpl.membershipDetails.get(NodeId).isIsFailed())) {
					NodeId = NodeImpl.preferenceList.getNextAvailableNode(topNList.get(0), lastNodeId);
					lastNodeId = NodeId;
				}

				if (NodeId == -1) {
					// No more running nodes left. Error out.
					break;

				}
			}
			final String Ip = NodeImpl.membershipDetails.get(NodeId).getIp();
			final int Port = NodeImpl.membershipDetails.get(NodeId).getPort();
			final int finalReplacedNodeId = replacedNodeId;
			Thread t = new Thread(new Runnable() {
				public void run() {
					INode inode;
					try {
						inode = (INode) Naming.lookup("rmi://" + Ip + ":" + Port + "/nodeServer");
						PutRequest.Builder prb = PutRequest.newBuilder(finalPRStream);

						prb.setIsReqFromLB(false);
						prb.setIsReqFromMediator(false);
						prb.setIsReqFromCoordinator(true);
						prb.setCoordinatorId(myNodeId);
						prb.setCoordinatorTimestamp(CoordinatorTimeStamp);
						prb.setReplacedNodeId(finalReplacedNodeId);
						PutResponse pr = PutResponse.parseFrom(inode.put(prb.build().toByteArray()));
						if (pr.getSuccess()) {
							doneCount.incrementAndGet();
						}

					} catch (MalformedURLException e) {
						e.printStackTrace();
					} catch (RemoteException e) {
						e.printStackTrace();
					} catch (NotBoundException e) {
						e.printStackTrace();
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}

				}
			});
			t.start();

		}
		while (true) {
			if (doneCount.get() >= W) {
				break;
			}
		}

		PutResponse.Builder putRespBuilder = PutResponse.newBuilder();
		putRespBuilder.setMsg("Success");
		putRespBuilder.setSuccess(true);
		return putRespBuilder.build().toByteArray();
	}

	// Served by Mediator
	private byte[] PutReqFromLB(byte[] inp) throws RemoteException, NotBoundException {
		PutRequest PRStream = null;
		String Key = "";

		try {
			PRStream = PutRequest.parseFrom(inp);
			Key = PRStream.getKey();
		} catch (InvalidProtocolBufferException e1) {
			e1.printStackTrace();
		}

		BigInteger KeyHash = utility_.md5(Key);
		int topNodeId = NodeImpl.preferenceList.getTopNode(KeyHash);
		if (NodeImpl.membershipDetails.get(topNodeId).isPossiblyFailed()
				|| NodeImpl.membershipDetails.get(topNodeId).isIsFailed()) {
			int startNodeId = topNodeId;
			while (topNodeId != -1 && (NodeImpl.membershipDetails.get(topNodeId).isPossiblyFailed()
					|| NodeImpl.membershipDetails.get(topNodeId).isIsFailed())) {
				topNodeId = NodeImpl.preferenceList.getNextAvailableNode(startNodeId, topNodeId);
			}

			if (topNodeId == -1) {
				// No more running nodes left. Error out.
				Utility.trace("PutReqFromLB :  Ran short of Nodes");
				PutResponse.Builder putRespBuilder = PutResponse.newBuilder();
				putRespBuilder.setMsg("Failure");
				putRespBuilder.setSuccess(false);
				return putRespBuilder.build().toByteArray();

			}
		}

		Utility.trace("The top node id is " + topNodeId);

		String Ip = NodeImpl.membershipDetails.get(topNodeId).getIp();
		int port = NodeImpl.membershipDetails.get(topNodeId).getPort();

		INode inode;
		try {
			inode = (INode) Naming.lookup("rmi://" + Ip + ":" + port + "/nodeServer");

			PutRequest.Builder prb = PutRequest.newBuilder(PRStream);

			prb.setIsReqFromCoordinator(false);
			prb.setIsReqFromMediator(true);
			prb.setIsReqFromLB(false);

			return inode.put(prb.build().toByteArray());

		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void putToDB(byte[] inp) {
		// key k.. value string =
		// v1:101-1-TS1,102-3-TS2,103-1-TS3$v2:101-2-TS4,102-2-TS5,103-1-TS$
		PutRequest PRStream = null;
		String Key = "";
		String Value = "";
		BigInteger KeyHash = null;
		int CoordinatorId = -1;
		long CoordinatorTimeStamp = -1;
		int replacedNodeId = -1;
		List<Vector> clientContextListUn = null;
		VersionValue.Builder clientVersionValue = null;
		DB db1 = null;

		byte[] valInDb = null;
		try {

			PRStream = PutRequest.parseFrom(inp);
			Key = PRStream.getKey();
			KeyHash = utility_.md5(Key);
			Value = PRStream.getValue();
			CoordinatorId = PRStream.getCoordinatorId();
			CoordinatorTimeStamp = PRStream.getCoordinatorTimestamp();
			replacedNodeId = PRStream.getReplacedNodeId();
			if (replacedNodeId != -1) {
				if (dbInstancesMap.containsKey(replacedNodeId)) {
					db1 = dbInstancesMap.get(replacedNodeId);
				} else {
					db1 = factory.open(new File("/opt/data/instance_" + myPort + "/DB_" + replacedNodeId),
							new Options());
					dbInstancesMap.put(replacedNodeId, db1);
				}

			} else {
				db1 = db;
			}
			String KeyHashString = KeyHash.toString();
			while (KeyHashString.length() < 39)
				KeyHashString = "0" + KeyHashString;
			valInDb = db1.get(bytes(KeyHashString));

			clientContextListUn = PRStream.getContextList();
			clientVersionValue = VersionValue.newBuilder();

			clientVersionValue.setValue(Value);
			boolean CoordinatorContextFound = false;
			for (Vector v : clientContextListUn) {
				if (v.getNodeId() == CoordinatorId) {
					CoordinatorContextFound = true;
					Vector.Builder vb = Vector.newBuilder();
					vb.setCounter(v.getCounter() + 1);
					vb.setNodeId(CoordinatorId);
					vb.setTimeStamp(CoordinatorTimeStamp);
					clientVersionValue.addContext(vb.build());
				} else {
					clientVersionValue.addContext(v);
				}
			}
			if (!CoordinatorContextFound) {
				Vector.Builder vb = Vector.newBuilder();
				vb.setCounter(1);
				vb.setNodeId(CoordinatorId);
				vb.setTimeStamp(System.currentTimeMillis());
				clientVersionValue.addContext(vb.build());
			}

		} catch (InvalidProtocolBufferException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		if (valInDb == null || valInDb.equals(null) || valInDb.length == 0) {
			Utility.trace("Value in db is null.. this is a fresh insert");
			Vector.Builder vectorBuilder = Vector.newBuilder();
			vectorBuilder.setCounter(1);
			vectorBuilder.setNodeId(CoordinatorId);
			vectorBuilder.setTimeStamp(CoordinatorTimeStamp);

			VersionValue.Builder verValueBuilder = VersionValue.newBuilder();
			verValueBuilder.setValue(Value);
			verValueBuilder.addContext(vectorBuilder);

			ConcValue.Builder valueBuilder = ConcValue.newBuilder();
			valueBuilder.addVersionValue(verValueBuilder);

			String finalKey = KeyHash.toString();
			while (finalKey.length() < 39)
				finalKey = "0" + finalKey;

			db1.put(bytes(finalKey), valueBuilder.build().toByteArray());

		} else {
			Utility.trace("A value exists in db....");
			List<Vector> clientContextList = new ArrayList<Vector>(clientContextListUn);

			Collections.sort(clientContextList, new VectorComparator());
			try {
				ConcValue dbConcValue = ConcValue.parseFrom(valInDb);

				List<VersionValue> dbVersionListUn = dbConcValue.getVersionValueList();
				List<VersionValue> dbVersionList = new ArrayList<VersionValue>(dbVersionListUn);
				List<Integer> ValuesToBeRemoved = new ArrayList<Integer>();

				Utility.trace("the size of dbversion list " + dbVersionList.size());
				int p = 0;
				for (VersionValue dbVersion : dbVersionList) {
					List<Vector> dbContextListUn = dbVersion.getContextList();
					List<Vector> dbContextList = new ArrayList<Vector>(dbContextListUn);
					Collections.sort(dbContextList, new VectorComparator());

					int size = dbContextList.size() + clientContextList.size();
					List<Boolean> isClientLater = new ArrayList<Boolean>(Arrays.asList(new Boolean[size]));
					List<Boolean> isDbLater = new ArrayList<Boolean>(Arrays.asList(new Boolean[size]));

					int i = 0, j = 0, counter = 0;
					while (!(i >= clientContextList.size() || j >= dbContextList.size())) {

						Vector clientVector = clientContextList.get(i);
						Vector dbVector = dbContextList.get(j);
						if (clientVector.getNodeId() == dbVector.getNodeId()) {
							if (clientVector.getCounter() > dbVector.getCounter()) {
								isClientLater.set(counter, true);
								isDbLater.set(counter, false);
								counter++;
								i++;
							} else if (clientVector.getCounter() < dbVector.getCounter()) {
								isDbLater.set(counter, true);
								isClientLater.set(counter, false);
								counter++;
								j++;
							} else {
								isClientLater.set(counter, true);
								isDbLater.set(counter, true);
								counter++;
								i++;
								j++;
							}
						}
						// db has sth that client doesn't
						// eg client - 103, db-101 => non causal.
						else if (dbVector.getNodeId() < clientVector.getNodeId()) {
							isDbLater.set(counter, true);
							isClientLater.set(counter, false);
							counter++;
							j++;
						}
						// client has sth that db doesn't
						// this may become a causal.. need to check for the next
						// node ids
						// eg: client : 101-2,102-3,103-2...
						// db : 102-2,103-2
						else {

							isClientLater.set(counter, true);
							isDbLater.set(counter, false);
							counter++;
							i++;
						}
					}

					while (i < clientContextList.size()) {
						isClientLater.set(counter, true);
						isDbLater.set(counter, false);
						counter++;
						i++;
					}

					while (j < dbContextList.size()) {
						isClientLater.set(counter, false);
						isDbLater.set(counter, true);
						counter++;
						j++;
					}

					boolean cl = true;
					boolean dl = true;
					for (int r = 0; r < counter; r++) {
						cl = cl && isClientLater.get(r);
						dl = dl && isDbLater.get(r);
					}

					if (cl) {
						ValuesToBeRemoved.add(p);
					}
					p++;
				}
				for (int k = ValuesToBeRemoved.size() - 1; k >= 0; k--) {
					dbVersionList.remove((int) ValuesToBeRemoved.get(k));
				}
				// so far we have removed from the db if we found a causal i.e
				// older event.
				// now we have to add the new version to the db. This is fine
				// even if we didn't find an older version
				// because a new version should always go to db.
				// before adding the new version , we need to check if the no.
				// of versions in the db have reached threshold
				// if yes remove the oldest

				dbVersionList.add(clientVersionValue.build());
				ConcValue.Builder concValBuilder = ConcValue.newBuilder();
				concValBuilder.addAllVersionValue(dbVersionList);
				String finalKey = KeyHash.toString();
				while (finalKey.length() < 39)
					finalKey = "0" + finalKey;
				db1.put(bytes(finalKey), concValBuilder.build().toByteArray());

			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}

		}

	}

	private byte[] reconcile(List<byte[]> resultStream) {
		HashSet<String> ResValue = new HashSet<String>();
		HashMap<Integer, Integer> contextMap = new HashMap<Integer, Integer>();
		HashMap<String, VersionValue> resultMap = new HashMap<String, VersionValue>();
		HashSet<String> ValuesToBeRemoved = new HashSet<String>();

		Utility.trace("reconciling the stream of size "+resultStream.size());
		GetResponse.Builder grb = GetResponse.newBuilder();
		try {
			// loop through output of each node
			for (int m = 0; m < resultStream.size(); m++) {
				if (resultStream.get(m) == null)
					continue;
				// output from each node v1+v2
				ConcValue curConcVal = ConcValue.parseFrom(resultStream.get(m));
				List<VersionValue> curVerValList = curConcVal.getVersionValueList();

				// loop through each version eg v1
				for (int k = 0; k < curVerValList.size(); k++) {
					VersionValue v = curVerValList.get(k);
					if (resultMap.containsKey(v.getValue())) {

						List<Vector> existingContentList = resultMap.get(v.getValue()).getContextList();
						List<Vector> tempList = new ArrayList<Vector>();
						tempList.addAll(v.getContextList());
						tempList.addAll(existingContentList);
						VersionValue.Builder vb = VersionValue.newBuilder();
						vb.addAllContext(tempList);
						resultMap.put(v.getValue(), vb.build());
					} else {
						resultMap.put(v.getValue(), v);
					}
					// get v1s context list
					List<Vector> curContextList = new ArrayList<Vector>(v.getContextList());
					Collections.sort(curContextList, new VectorComparator());

					// get nextnodes output and compare
					for (int n = m + 1; n < resultStream.size(); n++) {
						ConcValue nextConcVal = ConcValue.parseFrom(resultStream.get(n));
						List<VersionValue> nextVerValList = nextConcVal.getVersionValueList();

						// loop through v3 and v4
						for (int q = 0; q < nextVerValList.size(); q++) {
							VersionValue vn = nextVerValList.get(q);
							List<Vector> nextContextList = new ArrayList<Vector>(vn.getContextList());
							Collections.sort(nextContextList, new VectorComparator());

							boolean isCurVectorLater = false;
							boolean isNextVectorLater = false;
							boolean isNonCausal = false;
							int i = 0, j = 0;
							while (!(i >= curContextList.size() || j >= nextContextList.size())) {

								Vector curVector = curContextList.get(i);
								Vector nextVector = nextContextList.get(j);
								if (curVector.getNodeId() == nextVector.getNodeId()) {
									if (curVector.getCounter() > nextVector.getCounter()) {
										isCurVectorLater = true;
									} else if (curVector.getCounter() < nextVector.getCounter()) {
										isNextVectorLater = true;
									}
									i++;
									j++;
								}
								// eg baseVector - 103, nextVector -101 => non
								// causal.
								else if (nextVector.getNodeId() < curVector.getNodeId()) {
									isNonCausal = true;
									break;
								}
								// baseVector has sth that nextVector doesn't
								// this may become a causal.. need to check for
								// the next
								// node ids
								// eg: client : 101-2,102-3,103-2...
								// db : 102-2,103-2
								else {
									i++;
								}
							}
							if (isNonCausal || (isCurVectorLater && isNextVectorLater)) {
								continue;
							} else if (isNextVectorLater) {
								ValuesToBeRemoved.add(v.getValue());
								break;
							} else if (isCurVectorLater) {
								ValuesToBeRemoved.add(vn.getValue());
							}

						}

					}

				}
			}
			Iterator<String> i = ValuesToBeRemoved.iterator();
			while (i.hasNext()) {
				resultMap.remove(i.next());
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}

		for (Entry<String, VersionValue> entry : resultMap.entrySet()) {
			ResValue.add(entry.getKey());
			for (Vector v : entry.getValue().getContextList()) {
				if (contextMap.containsKey(v.getNodeId())) {
					if (v.getCounter() > contextMap.get(v.getNodeId())) {
						contextMap.put(v.getNodeId(), v.getCounter());
					}
				} else {
					contextMap.put(v.getNodeId(), v.getCounter());
				}
			}
		}

		Iterator<String> itr = ResValue.iterator();
		GetResult.Builder getresbuilder = GetResult.newBuilder();
		while (itr.hasNext()) {
			String val = (String) itr.next();
			getresbuilder.setValue(val);
			grb.addGetResults(getresbuilder.build());
		}

		Vector.Builder vb = Vector.newBuilder();
		for (Entry<Integer, Integer> e : contextMap.entrySet()) {
			vb.setNodeId(e.getKey());
			vb.setCounter(e.getValue());
			grb.addContext(vb.build());
		}
		return grb.build().toByteArray();
	}

	// write code to occasionally broadcast the list to all other nodes
	// This recovers network partitions. This can be ignored for now.

}

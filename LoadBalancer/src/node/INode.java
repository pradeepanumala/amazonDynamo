package node;

import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INode extends Remote { 

		/* getResponse get(getRequest)) */
		/* Method to get data  */
		byte[] get(byte[] inp) throws RemoteException;
		
		/* putResponse put(putRequest) */
		/* Method to write data */
		byte[] put(byte[] inp) throws RemoteException;	
		
		byte[] putDuringKeyMov(byte[] inp,  int coordinatorId) throws RemoteException;	
		
		/* sendGossip(sendGossipRequest) */
		/* Method to send gossip to other nodes */
		void hearGossip (byte[] inp) throws RemoteException;
	
		void putFile(byte[] inp, String extn) throws RemoteException; 
		
		void repairDb() throws RemoteException;
		
		void nodeDelInLB(Integer NodeId) throws RemoteException;
		
		boolean addNodeHandler(String newNodeIP, int newNodePort, BigInteger newNodeHash, BigInteger prevNodeHash) throws RemoteException;

		boolean hintedHandoff(String newNodeIP, int newNodePort, BigInteger newNodeHash, int newNodeID, boolean fromLB) throws RemoteException;
}

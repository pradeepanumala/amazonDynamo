package loadBalancer;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ILoadBalancer extends Remote{
	
 	public byte[] addNode(String Ip, int Port) throws RemoteException;
	
	public byte[] get(String Key) throws RemoteException;
	
	public String put(byte[] put) throws RemoteException;
	
	public boolean removeNode(Integer NodeId) throws RemoteException;

} 
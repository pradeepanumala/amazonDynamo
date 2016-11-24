package node;

import java.math.BigInteger;

public class MembershipInfoNode {
	private int NodeId;
	private BigInteger NodeHash;
	private int Port;
	private int Counter;
	private String Ip;
	private long SysTimestamp;
	private boolean PossiblyFailed; 
	private boolean IsFailed;
	



	public  MembershipInfoNode(int NodeId,BigInteger NodeHash,int Port,int Counter,
			String Ip,long SysTimestamp,boolean PossiblyFailed){
		this.setNodeId(NodeId);
		this.setNodeHash(NodeHash);
		this.setPort(Port);
		this.setCounter(Counter);
		this.setSysTimestamp(SysTimestamp);
		this.setIp(Ip);
		this.setPossiblyFailed(false);
		this.setIsFailed(false);
	}



	public int getCounter() {
		return Counter;
	}



	public void setCounter(int counter) {
		Counter = counter;
	}



	public int getNodeId() {
		return NodeId;
	}



	public void setNodeId(int nodeId) {
		NodeId = nodeId;
	}



	public long getSysTimestamp() {
		return SysTimestamp;
	}



	public void setSysTimestamp(long sysTimestamp) {
		SysTimestamp = sysTimestamp;
	}



	public boolean isPossiblyFailed() {
		return PossiblyFailed;
	}



	public void setPossiblyFailed(boolean possiblyFailed) {
		PossiblyFailed = possiblyFailed;
	}



	public String getIp() {
		return Ip;
	}



	public void setIp(String ip) {
		Ip = ip;
	}



	public BigInteger getNodeHash() {
		return NodeHash;
	}



	public void setNodeHash(BigInteger nodeHash) {
		NodeHash = nodeHash;
	}



	public int getPort() {
		return Port;
	}



	public void setPort(int port) {
		Port = port;
	}



	public boolean isIsFailed() {
		return IsFailed;
	}



	public void setIsFailed(boolean isFailed) {
		IsFailed = isFailed;
	}
}
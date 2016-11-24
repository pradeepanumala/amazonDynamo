package node;

import java.math.BigInteger;

public class PreferenceListNode {
	private int nodeId;
	private BigInteger nodeHash;
	public int getNodeId() {
		return nodeId;
	}
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	public BigInteger getNodeHash() {
		return nodeHash;
	}
	public void setNodeHash(BigInteger nodeHash) {
		this.nodeHash = nodeHash;
	}
	
	public PreferenceListNode(int nodeId,BigInteger nodeHash){
		this.nodeId=nodeId;
		this.nodeHash=nodeHash;
	}

	
}

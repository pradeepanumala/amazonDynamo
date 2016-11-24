package node;
import java.math.BigInteger;
// This is a data class. Stores all the details of the node
public class NodeDetails {

		private String ip;
		private int port;
		private BigInteger hash;
		
		public NodeDetails(String ip, int port, BigInteger hash){
			this.ip = ip;
			this.port = port;
			this.hash = hash;
		}
		
		public String getIp() {
			return ip;
		}

		public int getPort() {
			return port;
		}

		public BigInteger getHash() {
			return hash;
		}

	
}

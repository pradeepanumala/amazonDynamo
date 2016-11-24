package utility;
import java.util.Comparator;

import protobuf.DynamoProto;
import protobuf.DynamoProto.Vector;

public class VectorComparator implements Comparator<DynamoProto.Vector> {
		  @Override
		  public int compare(Vector x, Vector y) {
		    // TODO: Handle null x or y values
		    int startComparison = compare(x.getNodeId(), y.getNodeId());
		    return startComparison != 0 ? startComparison
		                                : compare(x.getNodeId(), y.getNodeId());
		  }

		  // I don't know why this isn't in Long...
		  private static int compare(int  a, int b) {
		    return a < b ? -1
		         : a > b ? 1
		         : 0;
		  }
}

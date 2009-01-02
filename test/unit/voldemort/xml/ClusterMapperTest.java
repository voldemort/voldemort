package voldemort.xml;

import java.io.StringReader;
import java.util.List;

import junit.framework.TestCase;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

public class ClusterMapperTest extends TestCase {

	public void testClusterMapperValues() {
		ClusterMapper mapper = new ClusterMapper();
		Cluster cluster = mapper.readCluster(new StringReader(VoldemortTestConstants.getOneNodeClusterXml()));
		assertEquals(cluster.getNumberOfNodes(), 1);
		assertEquals(cluster.getName(), "mycluster");
		Node node = cluster.getNodes().iterator().next();
		assertNotNull(node);
		assertEquals(node.getId(), 0);
		List<Integer> tags = node.getPartitionIds();
		assertTrue("Tag not found.", tags.contains(0));
		assertTrue("Tag not found.", tags.contains(1));
	}

	public void testOtherClusters() {
	    ClusterMapper mapper = new ClusterMapper();
	    mapper.readCluster(new StringReader(VoldemortTestConstants.getNineNodeClusterXml()));
	    mapper.readCluster(new StringReader(VoldemortTestConstants.getTwoNodeClusterXml()));
	}
}

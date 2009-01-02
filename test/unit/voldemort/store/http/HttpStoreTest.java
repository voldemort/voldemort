package voldemort.store.http;

import org.apache.commons.httpclient.HttpClient;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.ByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * @author jay
 *
 */
public class HttpStoreTest extends ByteArrayStoreTest {
	
	private HttpStore httpStore;
	private Server server;
	private Context context;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		Cluster cluster = VoldemortTestConstants.getOneNodeCluster();
		Node node = cluster.getNodes().iterator().next();
		context = ServerTestUtils.getJettyServer(VoldemortTestConstants.getOneNodeClusterXml(), 
		                                         VoldemortTestConstants.getSimpleStoreDefinitionsXml(), 
		                                         "users", 
		                                         node.getHttpPort());
		server = context.getServer();
		httpStore = ServerTestUtils.getHttpStore("users", node.getHttpPort());
	}
	
	public <T extends Exception> void testBadUrlOrPort(String url, int port, Class<T> expected) {
	    byte[] key = "test".getBytes();
		HttpStore badUrlHttpStore = new HttpStore("test", url, port, new HttpClient());
		try {
			badUrlHttpStore.put(key, new Versioned<byte[]>("value".getBytes(), new VectorClock()));
		} catch(Exception e) {
			assertTrue(e.getClass().equals(expected));
		}
		try {
			badUrlHttpStore.get(key);
		} catch(Exception e) {
			assertTrue(e.getClass().equals(expected));
		}
		try {
			badUrlHttpStore.delete(key, new VectorClock());
		} catch(Exception e) {
			assertTrue(e.getClass().equals(expected));
		}
	}
	
	public void testBadUrl() {
		testBadUrlOrPort("asfgsadfsda", 80, UnreachableStoreException.class);
	}
	
	public void testBadPort() {
		testBadUrlOrPort("localhost", 7777, UnreachableStoreException.class);
	}
	
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		httpStore.close();
		server.stop();
		context.destroy();
	}
	
	@Override
	public Store<byte[],byte[]> getStore() {
		return httpStore;
	}

}

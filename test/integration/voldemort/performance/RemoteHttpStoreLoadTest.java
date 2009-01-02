package voldemort.performance;

import voldemort.client.HttpStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.utils.Props;

public class RemoteHttpStoreLoadTest extends AbstractLoadTestHarness {

	@Override
	public StoreClient<String,String> getStore(Props propsA, Props propsB) throws java.lang.Exception {
		System.out.println("Initializing master server.");
		VoldemortServer serverA = new VoldemortServer(new VoldemortConfig(propsA));
		System.out.println("Initializing slave server.");
		VoldemortServer serverB = new VoldemortServer(new VoldemortConfig(propsB));
	    
		serverA.start();
	    serverB.start();
	    
	    HttpStoreClientFactory factory = new HttpStoreClientFactory(5, 2000, 2000, 0, 2000, 2000, 10000, 10, 5, serverA.getIdentityNode().getHttpUrl().toString());
		return factory.getStoreClient("users");
	}
	
	public static void main(String[] args) throws Exception {
		new RemoteHttpStoreLoadTest().run(args);
	}

}

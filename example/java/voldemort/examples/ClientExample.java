package voldemort.examples;

import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

public class ClientExample {

    public static void main(String[] args) {
	
	// In real life this stuff would get wired in
	int numThreads = 10;
        int maxQueuedRequests = 10;
        int maxConnectionsPerNode = 10; 
        int maxTotalConnections = 100;
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = 
            new SocketStoreClientFactory(numThreads,
        	    			 numThreads, 
        	    			 maxQueuedRequests, 
        	    			 maxConnectionsPerNode, 
        	    			 maxTotalConnections, 
        	    			 bootstrapUrl);
        
        StoreClient<String,String> client = factory.getStoreClient("my_store_name");
        
        // get the value
        Versioned<String> version = client.get("some_key");
        
        // modify the value
        version.setObject("new_value");
        
        // update the value
        client.put("some_key", version);
    }
	
}

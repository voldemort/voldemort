package voldemort.performance;

import voldemort.client.StoreClient;
import voldemort.server.VoldemortServer;
import voldemort.server.VoldemortConfig;
import voldemort.utils.Props;

public class SemiLocalHttpStoreLoadTest extends AbstractLoadTestHarness {

    @Override
    public StoreClient<String,String> getStore(Props propsA, Props propsB) throws java.lang.Exception {
        System.out.println("Initializing master server.");
        VoldemortServer serverA = new VoldemortServer(new VoldemortConfig(propsA));
        System.out.println("Initializing slave server.");
        VoldemortServer serverB = new VoldemortServer(new VoldemortConfig(propsB));
        
        serverA.start();
        serverB.start();
        
        return null;
//        Clients.getSemiLocalRoutedHttpStoreClient(Collections.singletonList(serverA.getIdentityNode().getHttpUrl()),
//                                                         serverA.getStoreConfiguration(),
//                                                         "users", 
//                                                         new StringSerializer("UTF-8"), 
//                                                         serverA.getIdentityNode().getId(), 
//                                                         20);
    }
    
    public static void main(String[] args) throws Exception {
        new RemoteHttpStoreLoadTest().run(args);
    }

}

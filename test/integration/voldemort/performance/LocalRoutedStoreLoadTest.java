package voldemort.performance;

import java.io.File;
import java.io.FileReader;
import java.util.Map;

import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.Store;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.routed.RoutedStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;
import voldemort.utils.Props;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Maps;

public class LocalRoutedStoreLoadTest extends AbstractLoadTestHarness {

	@Override
	public StoreClient<String,String> getStore(Props propsA, Props propsB) throws Exception {
		Cluster cluster = new ClusterMapper().readCluster(new FileReader(propsA.getString("metadata.directory") +
				File.separator + "/cluster.xml"));
		HashFunction hasher = new FnvHashFunction();
		RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(), 1);
		Map<Integer,Store<byte[],byte[]>> clientMapping = Maps.newHashMap();
		StorageConfiguration conf = new BdbStorageConfiguration(new VoldemortConfig(propsA));
		for(Node node: cluster.getNodes())
			clientMapping.put(node.getId(), conf.getStore("test" + node.getId()));
		
		InconsistencyResolver<Versioned<String>> resolver = new VectorClockInconsistencyResolver<String>();
		Store<byte[],byte[]> store = new RoutedStore("test", 
        											  clientMapping,
        											  routingStrategy,  
        											  1,
        											  1,
        											  10,
                                                      true,
                                                      10000L);
		Store<String,String> serializingStore = new SerializingStore<String,String>(store, new StringSerializer(), new StringSerializer());
		return new DefaultStoreClient<String, String>(new InconsistencyResolvingStore<String, String>(serializingStore, resolver),
                                        		        new StringSerializer(),
                                        		        new StringSerializer(),
                                        		        null);
	}

	public static void main(String[] args) throws Exception {
		new LocalRoutedStoreLoadTest().run(args);
	}
	
}

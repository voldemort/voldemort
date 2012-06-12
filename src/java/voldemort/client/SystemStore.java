package voldemort.client;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.system.SystemStoreConstants;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class SystemStore<K, V> {

    private final Logger logger = Logger.getLogger(DefaultStoreClient.class);
    private final SocketStoreClientFactory systemStoreFactory;
    private final String storeName;
    private volatile Store<K, V, Object> sysStore;

    public SystemStore(String storeName, String[] bootstrapUrls, int clientZoneID) {
        String prefix = storeName.substring(0, SystemStoreConstants.NAME_PREFIX.length());
        if(!SystemStoreConstants.NAME_PREFIX.equals(prefix))
            throw new VoldemortException("Illegal system store : " + storeName);

        ClientConfig config = new ClientConfig();
        config.setSelectors(1)
              .setBootstrapUrls(bootstrapUrls)
              .setMaxConnectionsPerNode(2)
              .setConnectionTimeout(1500, TimeUnit.MILLISECONDS)
              .setSocketTimeout(5000, TimeUnit.MILLISECONDS)
              .setRoutingTimeout(5000, TimeUnit.MILLISECONDS)
              .setEnableJmx(false)
              .setEnablePipelineRoutedStore(true)
              .setClientZoneId(clientZoneID);
        this.systemStoreFactory = new SocketStoreClientFactory(config);
        this.storeName = storeName;
        this.sysStore = this.systemStoreFactory.getSystemStore(this.storeName);
    }

    public void putSysStore(K key, V value) {
        try {
            logger.debug("Invoking Put for key : " + key + " on store name : " + this.storeName);
            Versioned<V> versioned = getSysStore(key);
            if(versioned == null)
                versioned = Versioned.value(value, new VectorClock());
            else
                versioned.setObject(value);
            this.sysStore.put(key, versioned, null);
        } catch(Exception e) {
            logger.info("Exception caught during putSysStore:");
            e.printStackTrace();
        }
    }

    public void putSysStore(K key, Versioned<V> value) {
        try {
            logger.debug("Invoking Put for key : " + key + " on store name : " + this.storeName);
            this.sysStore.put(key, value, null);
        } catch(Exception e) {
            logger.info("Exception caught during putSysStore:");
            e.printStackTrace();
        }
    }

    public Versioned<V> getSysStore(K key) {
        logger.debug("Invoking Get for key : " + key + " on store name : " + this.storeName);
        Versioned<V> versioned = null;
        try {
            List<Versioned<V>> items = this.sysStore.get(key, null);
            if(items.size() == 1)
                versioned = items.get(0);
            else if(items.size() > 1)
                throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                    + ") = " + items, items);
            if(versioned != null)
                logger.debug("Value for key : " + key + " = " + versioned.getValue()
                             + " on store name : " + this.storeName);
            else
                logger.debug("Got null value");
        } catch(Exception e) {
            logger.info("Exception caught during getSysStore:");
            e.printStackTrace();
        }
        return versioned;
    }

    public V getValueSysStore(K key) {
        V value = null;
        try {
            logger.debug("Invoking Get for key : " + key + " on store name : " + this.storeName);
            Versioned<V> versioned = getSysStore(key);
            if(versioned != null) {
                logger.debug("Value for key : " + key + " = " + versioned.getValue()
                             + " on store name : " + this.storeName);
                value = versioned.getValue();
            }
        } catch(Exception e) {
            logger.info("Exception caught during getSysStore:");
            e.printStackTrace();
        }
        return value;
    }
}

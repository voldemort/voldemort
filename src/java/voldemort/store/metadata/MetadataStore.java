package voldemort.store.metadata;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.Entry;
import voldemort.store.ObsoleteVersionException;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class MetadataStore implements StorageEngine<byte[], byte[]> {

    public static final String METADATA_STORE_NAME = "metadata";
    public static final String CLUSTER_KEY = "cluster.xml";
    public static final String STORES_KEY = "stores.xml";

    private final Store<String, String> innerStore;
    private final ClusterMapper clusterMapper;
    private final StoreDefinitionsMapper storeMapper;
    private final Map<String, ? extends Store<byte[], byte[]>> stores;

    public MetadataStore(Store<String, String> innerStore,
                         Map<String, ? extends Store<byte[], byte[]>> stores) {
        this.innerStore = innerStore;
        this.storeMapper = new StoreDefinitionsMapper();
        this.clusterMapper = new ClusterMapper();
        this.stores = stores;
    }

    public String getName() {
        return METADATA_STORE_NAME;
    }

    public boolean delete(byte[] key, Version version) throws VoldemortException {
        throw new VoldemortException("You can't delete your metadata, fool!");
    }

    /**
     * Store the new metadata, and apply any changes made by adding or deleting
     * stores
     */
    public void put(byte[] key, Versioned<byte[]> value) throws VoldemortException {
        synchronized(this) {
            String keyStr = ByteUtils.getString(key, "UTF-8");
            String valueStr = ByteUtils.getString(value.getValue(), "UTF-8");
            Versioned<String> newVersioned = new Versioned<String>(valueStr, value.getVersion());
            if(STORES_KEY.equals(key)) {
                List<Versioned<String>> current = innerStore.get(keyStr);
                if(current.size() == 0) {
                    // There are no current stores, so whatever they put is fine
                    innerStore.put(keyStr, newVersioned);
                } else if(current.size() == 1) {
                    // Okay there are current stores, so process the change
                    // JK: this shouldn't be necessary, right? The inner store
                    // should do this...
                    Versioned<String> versioned = current.get(0);
                    if(versioned.getVersion().compare(value.getVersion()) != Occured.BEFORE)
                        throw new ObsoleteVersionException("Attempt to put out of date store metadata!");
                    handleStoreChange(storeMapper.readStoreList(new StringReader(valueStr)));
                    innerStore.put(keyStr, newVersioned);
                } else {
                    throw new VoldemortException("Inconsistent metadata: " + current);
                }
            } else if(CLUSTER_KEY.equals(key)) {
                // TODO: handle cluster metadata updates
                innerStore.put(keyStr, newVersioned);
            }
        }
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

    public List<Versioned<byte[]>> get(byte[] key) throws VoldemortException {
        List<Versioned<byte[]>> values = Lists.newArrayList();
        for(Versioned<String> versioned : innerStore.get(ByteUtils.getString(key, "UTF-8")))
            values.add(new Versioned<byte[]>(ByteUtils.getBytes(versioned.getValue(), "UTF-8"),
                                             versioned.getVersion()));
        return values;
    }

    /**
     * Process a stores metadata change.
     * 
     * @param oldStores The list of old store definitions
     * @param newStores The list of new store definitions
     */
    private void handleStoreChange(List<StoreDefinition> newStores) {
        throw new VoldemortException("Not yet supported.");
        // synchronized(this) {
        // Set<String> deleted = new HashSet<String>(stores.keySet());
        // for(StoreDefinition store: newStores) {
        // if(stores.containsKey(store.getName()))
        // deleted.remove(store.getName());
        // //else
        // //stores.put(store.getName(), (Store<byte[],byte[]>)
        // storageConfiguration.getStore(store.getName()));
        // }
        //    
        // // Okay, now everything remaining in the deleted set is in the stores
        // map but not in the new XML
        // // these need to be deleted.
        // for(String name: deleted) {
        // Store<byte[],byte[]> store = stores.get(name);
        // stores.remove(name);
        // store.close();
        // }
        // }
    }

    public Cluster getCluster() {
        return clusterMapper.readCluster(new StringReader(getSingleValue(get(ByteUtils.getBytes(CLUSTER_KEY,
                                                                                                "UTF-8")))));
    }

    public List<StoreDefinition> getStores() {
        return storeMapper.readStoreList(new StringReader(getSingleValue(get(ByteUtils.getBytes(STORES_KEY,
                                                                                                "UTF-8")))));
    }

    private String getSingleValue(List<Versioned<byte[]>> found) {
        if(found.size() != 1)
            throw new VoldemortException("Inconsistent metadata found: expected 1 version but found "
                                         + found.size());
        return ByteUtils.getString(found.get(0).getValue(), "UTF-8");
    }

    public ClosableIterator<Entry<byte[], Versioned<byte[]>>> entries() {
        throw new IllegalStateException("Not implemented.");
    }

}

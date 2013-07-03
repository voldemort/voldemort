package voldemort.store.rebalancing;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import voldemort.cluster.Cluster;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.RoutingStrategy;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStoreListener;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;

/**
 * A listener on the metadatastore which the Redirecting store subscribes to for
 * proxy operations
 */
public class ProxyMetadataListener implements MetadataStoreListener {

    private RoutingStrategy newRoutingStrategy;
    private RoutingStrategy oldRoutingStrategy;

    private BaseStoreRoutingPlan oldRoutingPlan;
    private BaseStoreRoutingPlan currentRoutingPlan;

    private final String storeName;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    public final Lock readLock = lock.readLock();
    public final Lock writeLock = lock.writeLock();

    public BaseStoreRoutingPlan getOldRoutingPlan() {

        try {
            readLock.lock();
            return oldRoutingPlan;
        } finally {
            readLock.unlock();
        }

    }

    public BaseStoreRoutingPlan getCurrentRoutingPlan() {
        try {
            readLock.lock();
            return currentRoutingPlan;
        } finally {
            readLock.unlock();
        }
    }

    public RoutingStrategy getCurrentRoutingStrategy() {
        try {
            readLock.lock();
            return newRoutingStrategy;
        } finally {
            readLock.unlock();
        }
    }

    public RoutingStrategy getOldRoutingStrategy() {
        try {
            readLock.lock();
            return oldRoutingStrategy;
        } finally {
            readLock.unlock();
        }
    }

    public ProxyMetadataListener(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void updateRoutingStrategy(RoutingStrategy updatedRoutingStrategy) {
        return;
    }

    @Override
    public void updateStoreDefinition(StoreDefinition storeDef) {
        return;
    }

    @Override
    public void updateMetadataKeys(List<Pair<String, Object>> metadataKeyValueList) {

        Cluster cluster = null;
        List<StoreDefinition> storeDefs = null;

        Cluster oldCluster = null;
        List<StoreDefinition> oldStoreDefs = null;

        Iterator<Pair<String, Object>> iterator = metadataKeyValueList.iterator();
        while(iterator.hasNext()) {
            Pair<String, Object> metadataKeyValue = iterator.next();
            String key = metadataKeyValue.getFirst();
            Object value = metadataKeyValue.getSecond();

            if(MetadataStore.CLUSTER_KEY.equals(key)) {
                cluster = (Cluster) value;
            }

            if(MetadataStore.STORES_KEY.equals(key)) {
                storeDefs = (List<StoreDefinition>) value;
            }

            if(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML.equals(key)) {
                oldCluster = (Cluster) value;
            }

            if(MetadataStore.REBALANCING_SOURCE_STORES_XML.equals(key)) {
                oldStoreDefs = (List<StoreDefinition>) value;
            }
        }

        try {
            writeLock.lock();

            StoreDefinition def = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                  storeName);

            if(def != null) {
                currentRoutingPlan = new BaseStoreRoutingPlan(cluster, def);

            }

            if(oldStoreDefs != null && oldCluster != null) {

                StoreDefinition oldStoreDef = StoreDefinitionUtils.getStoreDefinitionWithName(oldStoreDefs,
                                                                                              storeName);
                if(oldStoreDef != null) {
                    oldRoutingPlan = new BaseStoreRoutingPlan(oldCluster, oldStoreDef);

                }

            }
        } finally {
            writeLock.unlock();
        }

    }
}

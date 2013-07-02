package voldemort.store.rebalancing;

import java.util.Iterator;
import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.RoutingStrategy;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStoreListener;
import voldemort.utils.Pair;

/*
 * A listener on the metadastore which the Redirecting store subsribes to for
 * proxy puts
 */
public class ProxyPutMetadataListener implements MetadataStoreListener {

    private RoutingStrategy newRoutingStrategy;
    private RoutingStrategy oldRoutingStrategy;

    private BaseStoreRoutingPlan oldRoutingPlan;
    private BaseStoreRoutingPlan newRoutingPlan;

    private final String storeName;

    public BaseStoreRoutingPlan getOldRoutingPlan() {
        return oldRoutingPlan;
    }

    public BaseStoreRoutingPlan getNewRoutingPlan() {
        return newRoutingPlan;
    }

    public RoutingStrategy getNewRoutingStrategy() {
        return newRoutingStrategy;
    }

    public RoutingStrategy getOldRoutingStrategy() {
        return oldRoutingStrategy;
    }

    public ProxyPutMetadataListener(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void updateRoutingStrategy(RoutingStrategy updatedRoutingStrategy) {
        oldRoutingStrategy = newRoutingStrategy;
        newRoutingStrategy = updatedRoutingStrategy;
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

        for(StoreDefinition def: storeDefs) {
            if(def.getName().equalsIgnoreCase(storeName)) {
                newRoutingPlan = new BaseStoreRoutingPlan(cluster, def);
                break;
            }
        }

        if(oldStoreDefs != null && oldCluster != null) {
            for(StoreDefinition def: oldStoreDefs) {
                if(def.getName().equalsIgnoreCase(storeName)) {
                    oldRoutingPlan = new BaseStoreRoutingPlan(oldCluster, def);
                    break;
                }
            }
        }

    }
}

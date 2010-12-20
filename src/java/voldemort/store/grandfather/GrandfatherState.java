package voldemort.store.grandfather;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Same as the RebalancerState but also maintains a mapping of partitions being
 * moved to the node ids. Required for fast lookup during grandfathering
 * 
 */
public class GrandfatherState extends RebalancerState {

    private final HashMultimap<Integer, Integer> partitionToNodeIds = HashMultimap.create();
    private List<StoreDefinition> storeDefs;
    private HashMap<String, RoutingStrategy> routingMap = Maps.newHashMap();
    private static final RoutingStrategyFactory routingFactory = new RoutingStrategyFactory();
    private Cluster cluster;

    public GrandfatherState(List<RebalancePartitionsInfo> stealInfoList,
                            List<StoreDefinition> storeDefs,
                            Cluster cluster) {
        super(stealInfoList);
        for(RebalancePartitionsInfo info: stealInfoList) {
            for(int partition: info.getPartitionList()) {
                Set<Integer> nodeIds = partitionToNodeIds.get(partition);
                if(nodeIds == null) {
                    nodeIds = Sets.newHashSet();
                    partitionToNodeIds.putAll(partition, nodeIds);
                }
                nodeIds.add(info.getStealerId());
            }
        }
        this.storeDefs = storeDefs;
        this.cluster = cluster;
        if(storeDefs != null && cluster != null) {
            for(StoreDefinition storeDef: storeDefs) {
                this.routingMap.put(storeDef.getName(),
                                    routingFactory.updateRoutingStrategy(storeDef, cluster));
            }
        }
    }

    public RoutingStrategy getRoutingStrategy(String storeName) {
        return routingMap.get(storeName);
    }

    public GrandfatherState(List<RebalancePartitionsInfo> stealInfoList,
                            String storeDefsString,
                            String clusterString) {
        this(stealInfoList,
             new StoreDefinitionsMapper().readStoreList(new StringReader(storeDefsString)),
             new ClusterMapper().readCluster(new StringReader(clusterString)));
    }

    public static GrandfatherState create(String json) {
        List<RebalancePartitionsInfo> stealInfoList = Lists.newLinkedList();
        JsonReader reader = new JsonReader(new StringReader(json));

        Map<String, ?> map = reader.readObject();
        String storeDefsString = Utils.uncheckedCast(map.get("storedef"));
        String clusterString = Utils.uncheckedCast(map.get("cluster"));
        List<Map<String, Object>> plans = Utils.uncheckedCast(map.get("plan"));

        for(Map<String, Object> o: plans) {
            stealInfoList.add(RebalancePartitionsInfo.create(o));
        }

        return new GrandfatherState(stealInfoList, storeDefsString, clusterString);
    }

    public Set<Integer> findNodeIds(int partition) {
        return partitionToNodeIds.get(partition);
    }

    @Override
    public String toJsonString() {
        List<Map<String, Object>> plansMap = Lists.newLinkedList();

        for(RebalancePartitionsInfo rebalancePartitionsInfo: stealInfoMap.values())
            plansMap.add(rebalancePartitionsInfo.asMap());

        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);

        Map<String, Object> map = Maps.newHashMap();
        map.put("storedef", new StoreDefinitionsMapper().writeStoreList(storeDefs));
        map.put("cluster", new ClusterMapper().writeCluster(cluster));
        map.put("plan", plansMap);

        jsonWriter.write(map);
        stringWriter.flush();

        return stringWriter.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;

        GrandfatherState that = (GrandfatherState) o;
        if(!this.stealInfoMap.equals(that.stealInfoMap))
            return false;
        if(!this.storeDefs.equals(that.storeDefs))
            return false;
        if(!this.cluster.equals(that.cluster))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = stealInfoMap.hashCode();
        result = 31 * result + storeDefs.hashCode();
        result = 31 * result + cluster.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("GrandfatherState(operations: ");
        sb.append("\n");
        for(RebalancePartitionsInfo info: getAll()) {
            sb.append(info);
            sb.append("\n");
        }
        sb.append("), storeDefs - " + storeDefs + ", cluster - " + cluster);

        return sb.toString();
    }

}

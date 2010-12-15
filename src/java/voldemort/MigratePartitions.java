package voldemort;

import com.google.common.collect.Lists;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.Props;
import voldemort.utils.RebalanceUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MigratePartitions {

    private final Cluster currentCluster;
    private final Cluster targetCluster;
    private final AdminClient adminClient;
    private final List<StoreDefinition> storeDefs;
    private final VoldemortConfig voldemortConfig;

    public MigratePartitions(Cluster currentCluster,
                             Cluster targetCluster,
                             List<StoreDefinition> storeDefs,
                             AdminClient adminClient,
                             VoldemortConfig voldemortConfig) {
        this.currentCluster = currentCluster;
        this.targetCluster = targetCluster;
        this.storeDefs = storeDefs;
        this.adminClient = adminClient;
        this.voldemortConfig = voldemortConfig;
    }

    public void migrate(int stealerId) {
        RebalanceClusterPlan plan = new RebalanceClusterPlan(currentCluster,
                                                             targetCluster,
                                                             storeDefs,
                                                             false,
                                                             null);
        List<String> storeNames = Lists.newArrayList();
        for(StoreDefinition storeDef: storeDefs)
            storeNames.add(storeDef.getName());

        List<RebalancePartitionsInfo> infoList = plan.getRebalanceNodeTask(currentCluster,
                                                                           targetCluster,
                                                                           storeNames,
                                                                           stealerId,
                                                                           false);
        // TODO LOW: handle several donors at once
        for(String storeName: storeNames) {
            for(RebalancePartitionsInfo r: infoList) {
                int requestId = adminClient.migratePartitions(r.getDonorId(),
                                                              stealerId,
                                                              storeName,
                                                              r.getPartitionList(),
                                                              null);
                adminClient.waitForCompletion(stealerId,
                                              requestId,
                                              voldemortConfig.getRebalancingTimeout(),
                                              TimeUnit.SECONDS);
            }
        }
    }

    public static void main(String [] args) {

        if(args.length < 4) {
            System.err.println("Usage: java MigratePartitions \\\n" +
                               "\t current_cluster_xml \\\n" +
                               "\t target_cluster_xml \\\n" +
                               "\t stores_xml stealer_node_id \\\n");
            System.exit(-1);
        }

        String currentClusterFile = args[0];
        String targetClusterFile = args[1];
        String storesFile = args[2];
        int stealerNodeId = Integer.parseInt(args[3]);
        AdminClient adminClient = null;
        try {
            VoldemortConfig voldemortConfig = new VoldemortConfig(new Props());
            Cluster currentCluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(currentClusterFile)));
            adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                           currentCluster,
                                                                           1,
                                                                           1);
            Cluster targetCluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(targetClusterFile)));
            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storesFile)));
            MigratePartitions migratePartitions = new MigratePartitions(currentCluster,
                                                                        targetCluster,
                                                                        storeDefs,
                                                                        adminClient,
                                                                        voldemortConfig);
            migratePartitions.migrate(stealerNodeId);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if(adminClient != null)
                adminClient.stop();
        }
    }
}

package voldemort.client.rebalance;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.CmdUtils;
import voldemort.utils.Props;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MigratePartitions {

    private static Logger logger = Logger.getLogger(MigratePartitions.class);
    private final Cluster currentCluster;
    private final Cluster targetCluster;
    private final AdminClient adminClient;
    private final List<StoreDefinition> storeDefs;
    private List<Integer> stealerNodeIds;
    private final VoldemortConfig voldemortConfig;

    public MigratePartitions(Cluster currentCluster,
                             Cluster targetCluster,
                             List<StoreDefinition> storeDefs,
                             AdminClient adminClient,
                             VoldemortConfig voldemortConfig,
                             List<Integer> stealerNodeIds) {
        this.currentCluster = currentCluster;
        this.targetCluster = targetCluster;
        this.storeDefs = storeDefs;
        this.adminClient = adminClient;
        this.stealerNodeIds = stealerNodeIds;
        this.voldemortConfig = voldemortConfig;
    }

    public void migrate() {
        RebalanceClusterPlan plan = new RebalanceClusterPlan(currentCluster,
                                                             targetCluster,
                                                             storeDefs,
                                                             false,
                                                             null);

        HashMap<Integer, RebalanceNodePlan> rebalancingTaskQueue = plan.getRebalancingTaskQueuePerNode();
        if(stealerNodeIds == null) {
            stealerNodeIds = Lists.newArrayList(rebalancingTaskQueue.keySet());
        }

        /**
         * Lets move all the donor nodes into grandfathering state. First
         * generate all donor node ids
         */
        Set<Integer> donorNodeIds = Sets.newHashSet();
        for(int stealerNodeId: stealerNodeIds) {
            RebalanceNodePlan nodePlan = rebalancingTaskQueue.get(stealerNodeId);
            if(nodePlan == null)
                continue;
            for(RebalancePartitionsInfo info: nodePlan.getRebalanceTaskList()) {
                donorNodeIds.add(info.getDonorId());
            }
        }
        logger.info("Changing state of donor nodes " + donorNodeIds);

        /**
         * Lets retrieve the current store state
         */
        for(int donorNodeId: donorNodeIds) {
            Versioned<String> serverState = adminClient.getRemoteMetadata(donorNodeId,
                                                                          MetadataStore.SERVER_STATE_KEY);
            if(!serverState.getValue().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
                logger.error("Node " + donorNodeId
                             + " is not in normal state to perform grandfathering");
            }
        }

        /**
         * Do all the stealer nodes sequentially, while each store can be done
         * in parallel
         * 
         */
        for(int stealerNodeId: stealerNodeIds) {
            RebalanceNodePlan nodePlan = rebalancingTaskQueue.get(stealerNodeId);
            if(nodePlan == null) {
                logger.info("No plan for stealer node id " + stealerNodeId);
                continue;
            }
            List<RebalancePartitionsInfo> partitionInfo = nodePlan.getRebalanceTaskList();

            logger.info("Working on stealer node id " + stealerNodeId);
            for(StoreDefinition storeDef: storeDefs) {
                logger.info("- Working on store " + storeDef.getName());

                HashMap<Integer, Integer> nodeIdToRequestId = Maps.newHashMap();
                for(RebalancePartitionsInfo r: partitionInfo) {
                    logger.info("-- Started migration for donor node id " + r.getDonorId());
                    nodeIdToRequestId.put(r.getDonorId(),
                                          adminClient.migratePartitions(r.getDonorId(),
                                                                        stealerNodeId,
                                                                        storeDef.getName(),
                                                                        r.getPartitionList(),
                                                                        null));

                }

                // Now that we started parallel migration for one store, wait
                // for it to complete
                for(int nodeId: nodeIdToRequestId.keySet()) {
                    adminClient.waitForCompletion(stealerNodeId,
                                                  nodeIdToRequestId.get(nodeId),
                                                  voldemortConfig.getRebalancingTimeout(),
                                                  TimeUnit.SECONDS);
                    logger.info("-- Completed migration for donor node id "
                                + nodeIdToRequestId.get(nodeId));
                }
            }

        }
    }

    public static void main(String[] args) throws IOException {

        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("target-cluster-xml", "[REQUIRED] target cluster xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("stores-xml", "[REQUIRED] stores xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("cluster-xml", "[REQUIRED] cluster xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("stealer-node-ids", "Comma separated node ids [ Default - all]")
              .withRequiredArg()
              .ofType(Integer.class)
              .withValuesSeparatedBy(',');

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "cluster-xml",
                                               "stores-xml",
                                               "target-cluster-xml");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String targetClusterFile = (String) options.valueOf("target-cluster-xml");
        String currentClusterFile = (String) options.valueOf("cluster-xml");
        String storesFile = (String) options.valueOf("stores-xml");

        List<Integer> stealerNodeIds = null;
        if(options.has("stealer-node-ids")) {
            stealerNodeIds = Utils.uncheckedCast(options.valueOf("stealer-node-ids"));
        }

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
                                                                        voldemortConfig,
                                                                        stealerNodeIds);

            migratePartitions.migrate();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if(adminClient != null)
                adminClient.stop();
        }
    }
}

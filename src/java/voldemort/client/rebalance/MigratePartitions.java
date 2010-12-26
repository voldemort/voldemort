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

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.CmdUtils;
import voldemort.utils.Props;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MigratePartitions {

    private static Logger logger = Logger.getLogger(MigratePartitions.class);
    private final Cluster currentCluster;
    private final Cluster targetCluster;
    private final AdminClient adminClient;
    private final List<StoreDefinition> currentStoreDefs;
    private final List<StoreDefinition> targetStoreDefs;
    private final String targetStoreDefsString;
    private List<Integer> stealerNodeIds;
    private final VoldemortConfig voldemortConfig;

    public MigratePartitions(Cluster currentCluster,
                             Cluster targetCluster,
                             List<StoreDefinition> currentStoreDefs,
                             List<StoreDefinition> targetStoreDefs,
                             AdminClient adminClient,
                             VoldemortConfig voldemortConfig,
                             List<Integer> stealerNodeIds) {
        this.currentCluster = currentCluster;
        this.targetCluster = targetCluster;
        this.currentStoreDefs = currentStoreDefs;
        this.targetStoreDefs = targetStoreDefs;
        this.adminClient = adminClient;
        this.stealerNodeIds = stealerNodeIds;
        this.voldemortConfig = voldemortConfig;
        this.targetStoreDefsString = new StoreDefinitionsMapper().writeStoreList(targetStoreDefs);
    }

    public void migrate() {
        RebalanceClusterPlan plan = new RebalanceClusterPlan(currentCluster,
                                                             targetCluster,
                                                             currentStoreDefs,
                                                             targetStoreDefs,
                                                             false,
                                                             null);

        HashMap<Integer, RebalanceNodePlan> rebalancingTaskQueue = plan.getRebalancingTaskQueuePerNode();
        if(stealerNodeIds == null) {
            stealerNodeIds = Lists.newArrayList(rebalancingTaskQueue.keySet());
        }

        logger.info("Stealer nodes being worked on " + stealerNodeIds);

        /**
         * Lets move all the donor nodes into grandfathering state. First
         * generate all donor node ids and corresponding migration plans
         */
        HashMap<Integer, List<RebalancePartitionsInfo>> donorNodePlans = Maps.newHashMap();
        for(int stealerNodeId: stealerNodeIds) {
            RebalanceNodePlan nodePlan = rebalancingTaskQueue.get(stealerNodeId);
            if(nodePlan == null)
                continue;
            for(RebalancePartitionsInfo info: nodePlan.getRebalanceTaskList()) {
                List<RebalancePartitionsInfo> donorPlan = donorNodePlans.get(info.getDonorId());
                if(donorPlan == null) {
                    donorPlan = Lists.newArrayList();
                    donorNodePlans.put(info.getDonorId(), donorPlan);
                }
                donorPlan.add(info);
            }
        }

        logger.info("Changing state of donor nodes " + donorNodePlans.keySet());

        HashMap<Integer, Versioned<String>> donorState = Maps.newHashMap();
        try {
            for(int donorNodeId: donorNodePlans.keySet()) {
                logger.info("Transitioning " + donorNodeId + " to grandfathering state");
                Versioned<String> serverState = adminClient.updateGrandfatherMetadata(donorNodeId,
                                                                                      donorNodePlans.get(donorNodeId),
                                                                                      targetStoreDefsString);
                if(!serverState.getValue()
                               .equals(MetadataStore.VoldemortState.GRANDFATHERING_SERVER)) {
                    throw new VoldemortException("Node "
                                                 + donorNodeId
                                                 + " is not in normal state to perform grandfathering");
                }
                donorState.put(donorNodeId, serverState);
                logger.info("Successfully transitioned " + donorNodeId + " to grandfathering state");
            }

            /**
             * Do all the stealer nodes sequentially, while each store can be
             * done in parallel for all the respective donor nodes
             */
            for(int stealerNodeId: stealerNodeIds) {
                RebalanceNodePlan nodePlan = rebalancingTaskQueue.get(stealerNodeId);
                if(nodePlan == null) {
                    logger.info("No plan for stealer node id " + stealerNodeId);
                    continue;
                }
                List<RebalancePartitionsInfo> partitionInfo = nodePlan.getRebalanceTaskList();

                logger.info("Working on stealer node id " + stealerNodeId);
                for(String storeName: RebalanceUtils.getStoreNames(targetStoreDefs)) {
                    logger.info("- Working on store " + storeName);

                    HashMap<Integer, Integer> nodeIdToRequestId = Maps.newHashMap();
                    for(RebalancePartitionsInfo r: partitionInfo) {
                        logger.info("-- Started migration for donor node id " + r.getDonorId());
                        nodeIdToRequestId.put(r.getDonorId(),
                                              adminClient.migratePartitions(r.getDonorId(),
                                                                            stealerNodeId,
                                                                            storeName,
                                                                            r.getPartitionList(),
                                                                            null));

                    }

                    // Now that we started parallel migration for one store,
                    // wait for it to complete
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
        } finally {
            // Move all nodes in grandfathered state back to normal
            for(int donorNodeId: donorState.keySet()) {
                logger.info("Rolling back state of " + donorNodeId + " to normal");
                try {
                    VectorClock clock = (VectorClock) donorState.get(donorNodeId).getVersion();
                    adminClient.updateRemoteMetadata(donorNodeId,
                                                     MetadataStore.SERVER_STATE_KEY,
                                                     Versioned.value(MetadataStore.VoldemortState.NORMAL_SERVER.toString(),
                                                                     clock.incremented(donorNodeId,
                                                                                       System.currentTimeMillis())));
                } catch(Exception e) {
                    logger.error("Rolling back state for " + donorNodeId + " failed");
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
        parser.accepts("target-stores-xml", "stores xml file location if changed")
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
        String currentStoresFile = (String) options.valueOf("stores-xml");
        String targetStoresFile = currentStoresFile;

        if(options.has("target-stores-xml")) {
            targetStoresFile = (String) options.valueOf("target-stores-xml");
        }

        if(!Utils.isReadableFile(targetClusterFile) || !Utils.isReadableFile(currentClusterFile)
           || !Utils.isReadableFile(currentStoresFile) || !Utils.isReadableFile(targetStoresFile)) {
            System.err.println("Could not read metadata files from path provided");
            parser.printHelpOn(System.err);
            System.exit(1);
        }

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
            List<StoreDefinition> currentStoreDefs = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(currentStoresFile)));
            List<StoreDefinition> targetStoreDefs = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(targetStoresFile)));

            MigratePartitions migratePartitions = new MigratePartitions(currentCluster,
                                                                        targetCluster,
                                                                        currentStoreDefs,
                                                                        targetStoreDefs,
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

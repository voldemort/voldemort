package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.NativeAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;

/**
 * 
 * @author bbansal
 * 
 */
public class DefaultRebalanceClient implements RebalanceClient {

    private static final Logger logger = Logger.getLogger(DefaultRebalanceClient.class);
    private final NativeAdminClientRequestFormat adminClient;

    /**
     * 
     * @param connectedNodeId
     * @param metadata
     * @param socketPool: socket Timeout should be kept high as streaming is
     *        throttled by server min 10 sec +
     */
    public DefaultRebalanceClient(int connectedNodeId, MetadataStore metadata, SocketPool socketPool) {
        adminClient = new NativeAdminClientRequestFormat(metadata, socketPool);
    }

    public void stealPartitions(int stealerNodeID, String storeName, Cluster currentCluster) {
        throw new VoldemortException("Not supported yet.");
    }

    public void stealPartitions(int stealerNodeID,
                                String storeName,
                                Cluster currentCluster,
                                Cluster targetCluster) {
        logger.info("Node(" + stealerNodeID + ") Starting Steal Process");

        Node stealerNode = currentCluster.getNodeById(stealerNodeID);
        if(stealerNode != null) {
            try {
                // Set stealerNode state
                adminClient.updateServerState(stealerNodeID,
                                              MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);

                // stealer node sets targetCluster as currentCluster
                adminClient.updateClusterMetadata(stealerNodeID, targetCluster);

                // start stealing from all nodes one-by-one
                for(Node donorNode: currentCluster.getNodes()) {
                    if(donorNode.getId() != stealerNode.getId()) {

                        // identify steal partitions belonging to donorNode
                        List<Integer> stealList = getExtraPartitionList(donorNode.getPartitionIds(),
                                                                        targetCluster.getNodeById(donorNode.getId())
                                                                                     .getPartitionIds());

                        if(stealList.size() > 0) {
                            logger.info("stealing partitions from " + donorNode.getId());

                            // change state for donorNode
                            adminClient.updateServerState(donorNode.getId(),
                                                          MetadataStore.VoldemortState.REBALANCING_SLAVE_SERVER);

                            // donorNode sets new cluster
                            adminClient.updateClusterMetadata(donorNode.getId(), targetCluster);

                            adminClient.fetchAndUpdateStreams(donorNode.getId(),
                                                              stealerNodeID,
                                                              storeName,
                                                              stealList,
                                                              null);
                            // set donorNode back to Normal
                            adminClient.updateServerState(donorNode.getId(),
                                                          MetadataStore.VoldemortState.NORMAL_SERVER);
                        }
                    }
                }
                // everything kool change stealerState to be normal
                adminClient.updateServerState(stealerNodeID, MetadataStore.VoldemortState.NORMAL_SERVER);
            } catch(Exception e) {
                // if any node fails be paranoid and roll back everything
                for(Node node: currentCluster.getNodes()) {
                    adminClient.updateServerState(node.getId(),
                                                  MetadataStore.VoldemortState.NORMAL_SERVER);
                    adminClient.updateClusterMetadata(node.getId(), currentCluster);
                }
                throw new VoldemortException("Steal Partitions for " + stealerNodeID + " failed", e);
            }
        }
        logger.info("Node(" + stealerNodeID + ") Steal process completed");
    }

    /**
     * returns a list of partitions present in partitionIds1 and not present in
     * partitionIds2
     * 
     * @param partitionIds1
     * @param partitionIds2
     * @return extra partitions
     */
    private List<Integer> getExtraPartitionList(List<Integer> partitionIds1,
                                                List<Integer> partitionIds2) {
        List<Integer> diffList = new ArrayList<Integer>();
        for(Integer partitionId: partitionIds1) {
            if(!partitionIds2.contains(partitionId)) {
                diffList.add(partitionId);
            }
        }
        return diffList;
    }

    public void donatePartitions(int donorNodeId, String storeName, Cluster currentCluster) {
        throw new VoldemortException("Not supported yet.");
    }

    public void donatePartitions(int donorNodeId,
                                 String storeName,
                                 Cluster currentCluster,
                                 Cluster targetCluster) {
        logger.info("Node(" + donorNodeId + ") Starting Donate Process");

        Node donorNode = currentCluster.getNodeById(donorNodeId);
        if(donorNode != null) {
            try {
                // Set donorNode state
                adminClient.updateServerState(donorNodeId,
                                              MetadataStore.VoldemortState.REBALANCING_SLAVE_SERVER);

                // donorNode sets targetCluster as currentCluster
                adminClient.updateClusterMetadata(donorNodeId, targetCluster);

                // start stealing from all nodes one-by-one
                for(Node destNode: currentCluster.getNodes()) {
                    if(destNode.getId() != donorNode.getId()) {

                        // identify donate partitions destined to destNode
                        List<Integer> donateList = getExtraPartitionList(targetCluster.getNodeById(destNode.getId())
                                                                                      .getPartitionIds(),
                                                                         destNode.getPartitionIds());

                        if(donateList.size() > 0) {
                            logger.info("donating partitions to " + destNode.getId());

                            // change state for destNode
                            adminClient.updateServerState(destNode.getId(),
                                                          MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);

                            // destNode sets new cluster
                            adminClient.updateClusterMetadata(destNode.getId(), targetCluster);

                            adminClient.fetchAndUpdateStreams(donorNode.getId(),
                                                              destNode.getId(),
                                                              storeName,
                                                              donateList,
                                                              null);
                            // set destNode back to Normal
                            adminClient.updateServerState(destNode.getId(),
                                                          MetadataStore.VoldemortState.NORMAL_SERVER);
                        }
                    }
                }
                // everything kool change donorNode to be normal
                adminClient.updateServerState(donorNodeId, MetadataStore.VoldemortState.NORMAL_SERVER);
            } catch(Exception e) {
                // if any node fails be paranoid and roll back everything
                for(Node node: currentCluster.getNodes()) {
                    adminClient.updateServerState(node.getId(),
                                                  MetadataStore.VoldemortState.NORMAL_SERVER);
                    adminClient.updateClusterMetadata(node.getId(), currentCluster);
                }
                throw new VoldemortException("Donate Partitions for " + donorNodeId + " failed", e);
            }
        }
        logger.info("Node(" + donorNodeId + ") Donate process completed");
    }

}
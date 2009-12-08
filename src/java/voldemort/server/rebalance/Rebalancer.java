package voldemort.server.rebalance;

import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.protocol.admin.AsyncOperationRunner;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;

public class Rebalancer {

    /**
     * Rebalance logic at single node level.<br>
     * <imp> should be called by the rebalancing node itself</imp><br>
     * Attempt to rebalance from node {@link RebalanceStealInfo#getDonorId()}
     * for partitionList {@link RebalanceStealInfo#getPartitionList()}
     * <p>
     * Force Sets serverState to rebalancing, Sets stealInfo in MetadataStore,
     * fetch keys from remote node and upsert them locally.<br>
     * On success clean all states it changed
     * 
     * @param metadataStore
     * @param stealInfo
     * @return taskId for asynchronous task.
     */
    public int rebalanceLocalNode(final MetadataStore metadataStore,
                                  final String storeName,
                                  final RebalanceStealInfo stealInfo,
                                  final AsyncOperationRunner asyncRunner,
                                  final AdminClient adminClient) {
        int requestId = asyncRunner.getUniqueRequestId();
        asyncRunner.submitOperation(requestId,
                                    new AsyncOperation(requestId, "rebalanceNode:"
                                                                  + stealInfo.toString()) {

                                        private int fetchAndUpdateAsyncId = -1;

                                        @Override
                                        public void operate() throws Exception {
                                            synchronized(metadataStore) {
                                                checkCurrentState(metadataStore, stealInfo);
                                                setRebalancingState(metadataStore, stealInfo);
                                            }
                                            fetchAndUpdateAsyncId = adminClient.fetchAndUpdateStreams(metadataStore.getNodeId(),
                                                                                                      stealInfo.getDonorId(),
                                                                                                      storeName,
                                                                                                      stealInfo.getPartitionList(),
                                                                                                      null);
                                            adminClient.waitForCompletion(metadataStore.getNodeId(),
                                                                          fetchAndUpdateAsyncId,
                                                                          24 * 60 * 60,
                                                                          TimeUnit.SECONDS);

                                            metadataStore.cleanAllRebalancingState();
                                        }

                                        @Override
                                        @JmxGetter(name = "asyncTaskStatus")
                                        public AsyncOperationStatus getStatus() {
                                            return adminClient.getAsyncRequestStatus(metadataStore.getNodeId(),
                                                                                     fetchAndUpdateAsyncId);
                                        }

                                        private void setRebalancingState(MetadataStore metadataStore,
                                                                         RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            metadataStore.put(MetadataStore.SERVER_STATE_KEY,
                                                              VoldemortState.REBALANCING_MASTER_SERVER);
                                            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO,
                                                              stealInfo);
                                        }

                                        private void checkCurrentState(MetadataStore metadataStore,
                                                                       RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            if(metadataStore.getServerState()
                                                            .equals(VoldemortState.REBALANCING_MASTER_SERVER)
                                               && metadataStore.getRebalancingStealInfo()
                                                               .getDonorId() != stealInfo.getDonorId())
                                                throw new VoldemortException("Server "
                                                                             + metadataStore.getNodeId()
                                                                             + " is already rebalancing from:"
                                                                             + metadataStore.getRebalancingStealInfo()
                                                                             + " rejecting rebalance request:"
                                                                             + stealInfo);
                                        }

                                    });

        return requestId;
    }
}
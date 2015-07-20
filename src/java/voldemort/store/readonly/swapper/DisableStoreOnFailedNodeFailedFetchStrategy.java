package voldemort.store.readonly.swapper;

import com.google.common.collect.Sets;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

import java.util.Map;
import java.util.Set;

/**
*
*/
public class DisableStoreOnFailedNodeFailedFetchStrategy extends FailedFetchStrategy {
    private final FailedFetchLock distributedLock;
    private final Integer maxNodeFailure;
    private final String extraInfo;

    public DisableStoreOnFailedNodeFailedFetchStrategy(AdminClient adminClient,
                                                       FailedFetchLock distributedLock,
                                                       Integer maxNodeFailure,
                                                       String extraInfo) {
        super(adminClient);
        this.distributedLock = distributedLock;
        this.maxNodeFailure = maxNodeFailure;
        this.extraInfo = extraInfo;
    }

    @Override
    protected boolean dealWithIt(String storeName,
                                 long pushVersion,
                                 Map<Node, AdminStoreSwapper.Response> fetchResponseMap) throws Exception {
        int failureCount = 0;
        for (AdminStoreSwapper.Response response: fetchResponseMap.values()) {
            if (!response.isSuccessful()) {
                failureCount++;
            }
        }
        if (failureCount > maxNodeFailure) {
            // Too many exceptions to tolerate this strategy... let's bail out.
            logger.error("We cannot use " + getClass().getSimpleName() +
                    " because there is more than " + maxNodeFailure + " nodes that failed their fetches...");
            return false;
        } else {
            try {
                distributedLock.acquireLock();
                Set<Integer> alreadyDisabledNodes = distributedLock.getDisabledNodes();
                Set<Integer> nodesFailedInThisRun = Sets.newHashSet();

                for (Map.Entry<Node, AdminStoreSwapper.Response> entry: fetchResponseMap.entrySet()) {
                    if (!entry.getValue().isSuccessful()) {
                        nodesFailedInThisRun.add(entry.getKey().getId());
                    }
                }

                Set<Integer> allNodesToBeDisabled = Sets.newHashSet();
                allNodesToBeDisabled.addAll(alreadyDisabledNodes);
                allNodesToBeDisabled.addAll(nodesFailedInThisRun);

                if (allNodesToBeDisabled.size() > maxNodeFailure) {
                    // Too many exceptions to tolerate this strategy... let's bail out.
                    logger.error("We cannot use " + getClass().getSimpleName() +
                            " because it would bring the total number of nodes with" +
                            " disabled stores to more than " + maxNodeFailure + "...");
                    return false;
                }

                for (Integer nodeId: nodesFailedInThisRun) {
                    logger.warn("Will disable store '" + storeName + "' on node " + nodeId);
                    distributedLock.addDisabledNode(nodeId, storeName, pushVersion);
                    try {
                        adminClient.readonlyOps.disableStoreVersion(nodeId, storeName, pushVersion, extraInfo);
                    } catch (UnreachableStoreException e) {
                        logger.warn("Got an UnreachableStoreException while trying to disableStoreVersion on node " +
                                nodeId + ", store " + storeName + ", version " + pushVersion +
                                ". If the node is actually up and merely net-split from us, it might continue serving stale data...", e);
                    }
                }

                return true;
            } catch (Exception e) {
                logger.error(getClass().getSimpleName() + " got an exception while trying to dealWithIt.", e);
                return false;
            } finally {
                distributedLock.releaseLock();
            }
        }
    }
}

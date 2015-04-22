package voldemort.store.readonly.swapper;

import com.google.common.collect.Sets;
import voldemort.client.protocol.admin.AdminClient;

import java.util.Map;
import java.util.Set;

/**
*
*/
public class DisableStoreOnFailedNodeFailedFetchStrategy extends FailedFetchStrategy {
    private final FailedFetchLock distributedLock;
    private final Integer maxNodeFailure;

    public DisableStoreOnFailedNodeFailedFetchStrategy(AdminClient adminClient,
                                                       FailedFetchLock distributedLock,
                                                       Integer maxNodeFailure) {
        super(adminClient);
        this.distributedLock = distributedLock;
        this.maxNodeFailure = maxNodeFailure;
    }

    @Override
    protected boolean dealWithIt(String storeName,
                                 long pushVersion,
                                 Map<Integer, AdminStoreSwapper.Response> fetchResponseMap) throws Exception {
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

                for (Map.Entry<Integer, AdminStoreSwapper.Response> entry: fetchResponseMap.entrySet()) {
                    if (!entry.getValue().isSuccessful()) {
                        nodesFailedInThisRun.add(entry.getKey());
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
                    String details = "TBD";
                    distributedLock.addDisabledNode(nodeId, details);
                    adminClient.readonlyOps.disableStore(nodeId, storeName);
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

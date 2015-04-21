package voldemort.store.readonly.swapper;

import voldemort.client.protocol.admin.AdminClient;

import java.util.Map;

/**
*
*/
class DisableFailedOnlyFailedFetchStrategy extends FailedFetchStrategy {
    private static int MAX_NODE_FAILURES = 1;

    public DisableFailedOnlyFailedFetchStrategy(AdminClient adminClient) {
        super(adminClient);
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
        if (failureCount > MAX_NODE_FAILURES) {
            // Too many exceptions to tolerate this strategy... let's bail out.
            logger.error("We cannot use " + getClass().getSimpleName() +
                    " because there is more than " + MAX_NODE_FAILURES + " nodes that failed their fetches...");
            return false;
        } else {
            for (Map.Entry<Integer, AdminStoreSwapper.Response> entry: fetchResponseMap.entrySet()) {
                if (!entry.getValue().isSuccessful()) {
                    Integer nodeId = entry.getKey();
                    logger.warn("Will disable store '" + storeName + "' on node " + nodeId);
                    adminClient.readonlyOps.disableStore(nodeId, storeName);
                }
            }
            return true;
        }
    }
}

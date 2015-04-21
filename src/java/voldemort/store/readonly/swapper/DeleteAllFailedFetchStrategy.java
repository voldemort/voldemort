package voldemort.store.readonly.swapper;

import voldemort.client.protocol.admin.AdminClient;

import java.util.Map;

/**
*
*/
class DeleteAllFailedFetchStrategy extends FailedFetchStrategy {
    public DeleteAllFailedFetchStrategy(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    protected boolean dealWithIt(String storeName,
                                 long pushVersion,
                                 Map<Integer, AdminStoreSwapper.Response> fetchResponseMap) {
        // Delete data from successful nodes
        for(int nodeId: fetchResponseMap.keySet()) {
            AdminStoreSwapper.Response response = fetchResponseMap.get(nodeId);
            if (response.isSuccessful()) {
                try {
                    logger.info("Deleting fetched data from node " + nodeId);
                    adminClient.readonlyOps.failedFetchStore(nodeId, storeName, response.getResponse());
                } catch(Exception e) {
                    logger.error("Exception thrown during delete operation on node " + nodeId + " : ", e);
                }
            }
        }
        return false;
    }
}

package voldemort.store.readonly.swapper;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;

import java.util.Map;

/**
*
*/
public class DeleteAllFailedFetchStrategy extends FailedFetchStrategy {
    public DeleteAllFailedFetchStrategy(AdminClient adminClient) {
        super(adminClient);
    }

    @Override
    protected boolean dealWithIt(String storeName,
                                 long pushVersion,
                                 Map<Node, AdminStoreSwapper.Response> fetchResponseMap) {
        // Delete data from successful nodes
        for(Node node: fetchResponseMap.keySet()) {
            AdminStoreSwapper.Response response = fetchResponseMap.get(node);
            if (response.isSuccessful()) {
                try {
                    logger.info("Deleting fetched data from node " + node);
                    adminClient.readonlyOps.failedFetchStore(node.getId(), storeName, response.getResponse());
                } catch(Exception e) {
                    logger.error("Exception thrown during delete operation on node " + node + " : ", e);
                }
            }
        }
        return false;
    }
}

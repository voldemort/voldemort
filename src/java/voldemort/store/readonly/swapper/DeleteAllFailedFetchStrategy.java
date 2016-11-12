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
        // We attempt to delete data from all nodes, even the ones that failed their fetch.
        for (Node node: fetchResponseMap.keySet()) {
            AdminStoreSwapper.Response response = fetchResponseMap.get(node);
            String nodeDescription = node.briefToString() + " (which " +
                (response.isSuccessful() ? "succeeded" : "failed") + " on its fetch).";
            try {
                adminClient.readonlyOps.failedFetchStore(node.getId(), storeName, response.getResponse());
                logger.info("Deleted fetched data from " + nodeDescription);
            } catch(Exception e) {
                logger.error("Exception thrown during delete operation on " + nodeDescription, e);
            }
        }
        return false;
    }
}

package voldemort.store.readonly.swapper;

import java.util.List;
import java.util.Map;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.readonly.swapper.AdminStoreSwapper.Response;

import com.google.common.collect.Lists;

/**
*
*/
public class DisableStoreOnFailedNodeFailedFetchStrategy extends FailedFetchStrategy {
    private final String extraInfo;

    public DisableStoreOnFailedNodeFailedFetchStrategy(AdminClient adminClient,
                                                       String extraInfo) {
        super(adminClient);
        this.extraInfo = extraInfo;
    }

    @Override
    protected boolean dealWithIt(String storeName,
                                 long pushVersion,
                                 Map<Node, AdminStoreSwapper.Response> fetchResponseMap)
            throws Exception {
        int numQuotaExceptions = 0;
        List<Integer> failedNodes = Lists.newArrayList();
        for(Map.Entry<Node, AdminStoreSwapper.Response> entry: fetchResponseMap.entrySet()) {
            // Only consider non Quota related exceptions as Failures.
            Response response = entry.getValue();
            if(!response.isSuccessful()) {
                // Check if there are any exceptions due to Quota
                if(response.getException() instanceof QuotaExceededException) {
                    numQuotaExceptions++;
                } else {
                    failedNodes.add(entry.getKey().getId());
                }
            }
        }
        // QuotaException trumps all others
        if(numQuotaExceptions > 0) {
            logger.error("We cannot use "
                         + getClass().getSimpleName()
                         + " because there are QuotaExceededExceptions that caused fetch failures...");
            return false;
        }
        return adminClient.readonlyOps.handleFailedFetch(failedNodes,
                                                         storeName,
                                                         pushVersion,
                                                         extraInfo);
    }
}

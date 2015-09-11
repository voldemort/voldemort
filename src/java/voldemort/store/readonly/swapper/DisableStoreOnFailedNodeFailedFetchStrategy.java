package voldemort.store.readonly.swapper;

import com.google.common.collect.Lists;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;

import java.util.List;
import java.util.Map;

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
                                 Map<Node, AdminStoreSwapper.Response> fetchResponseMap) throws Exception {
        List<Integer> failedNodes = Lists.newArrayList();
        for (Map.Entry<Node, AdminStoreSwapper.Response> entry: fetchResponseMap.entrySet()) {
            if (!entry.getValue().isSuccessful()) {
                failedNodes.add(entry.getKey().getId());
            }
        }
        return adminClient.readonlyOps.handleFailedFetch(failedNodes, storeName, pushVersion, extraInfo);
    }
}

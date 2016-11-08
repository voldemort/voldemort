package voldemort.store.readonly.swapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.store.readonly.swapper.AdminStoreSwapper.Response;
import voldemort.utils.ExceptionUtils;

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
        List<String> hardFailures = new ArrayList<String>();
        List<Integer> failedNodes = Lists.newArrayList();
        for(Map.Entry<Node, AdminStoreSwapper.Response> entry: fetchResponseMap.entrySet()) {
            Response response = entry.getValue();
            if(!response.isSuccessful()) {
                Exception ex = response.getException();
                boolean isSoftError = ExceptionUtils.recursiveClassEquals(ex, ExceptionUtils.BNP_SOFT_ERRORS);

                Node node = entry.getKey();
                if(isSoftError) {
                    int nodeId = node.getId();
                    failedNodes.add(nodeId);
                } else {
                    String nodeErrorMessage = node.briefToString() + " threw exception "
                                              + ex.getClass().getName() + ". ";
                    hardFailures.add(nodeErrorMessage);
                }
            }
        }

        if(hardFailures.size() > 0) {
            String errorMessage = Arrays.toString(hardFailures.toArray());
            logger.error("There were non connection related errors. Details " + errorMessage);
            return false;
        }
        return adminClient.readonlyOps.handleFailedFetch(failedNodes,
                                                         storeName,
                                                         pushVersion,
                                                         extraInfo);
    }
}

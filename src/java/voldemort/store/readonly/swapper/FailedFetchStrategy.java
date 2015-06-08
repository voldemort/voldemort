package voldemort.store.readonly.swapper;

import org.apache.log4j.Logger;
import voldemort.client.protocol.admin.AdminClient;

import java.util.Map;

/**
* Class for dealing with fetch failures during Build And Push.
*/
public abstract class FailedFetchStrategy {
    protected final Logger logger = Logger.getLogger(this.getClass().getName());
    protected final AdminClient adminClient;

    public FailedFetchStrategy(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     *
     * @param storeName name of the store affected by the failed fetch
     * @param pushVersion version of the store affected by the failed fetch
     * @param fetchResponseMap map of <node ID, {@link voldemort.store.readonly.swapper.AdminStoreSwapper.Response}>
     * @return true if the store/version is in a condition where the swap can be done, false otherwise.
     * @throws Exception
     */
    protected abstract boolean dealWithIt(String storeName,
                                          long pushVersion,
                                          Map<Integer, AdminStoreSwapper.Response> fetchResponseMap) throws Exception;
}

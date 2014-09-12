package voldemort.rest.coordinator.config;

import voldemort.rest.coordinator.CoordinatorConfig;

import java.util.List;

/**
 * Stores configs on the local filesystem
 */
public class FileBasedStoreClientConfigService extends StoreClientConfigService {

    protected FileBasedStoreClientConfigService(CoordinatorConfig coordinatorConfig) {
        super(coordinatorConfig);
    }

    @Override
    protected String getAllConfigsImpl() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected String getSpecificConfigsImpl(List<String> storeNames) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

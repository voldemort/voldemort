package voldemort.rest.coordinator.config;

import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * Stores configs on the local filesystem
 */
public class FileBasedStoreClientConfigService extends StoreClientConfigService {

    private Logger logger = Logger.getLogger(this.getClass().toString());

    protected FileBasedStoreClientConfigService(CoordinatorConfig coordinatorConfig) {
        super(coordinatorConfig);
    }

    @Override
    protected String getAllConfigsImpl() {
        try {
            return Joiner.on("\n")
                         .join(IOUtils.readLines(new FileReader(new File(coordinatorConfig.getFatClientConfigPath()))))
                         .trim();
        } catch (IOException e) {
            logger.error("Problem reading the config file:\n", e);
            throw new RuntimeException("Problem reading the config file", e);
        }
    }

    @Override
    protected String getSpecificConfigsImpl(List<String> storeNames) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

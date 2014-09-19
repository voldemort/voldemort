package voldemort.rest.coordinator.config;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    protected String getSpecificConfigsImpl(List<String> requestedStoreNames) {
        Map<String, Properties> allConfigs = ClientConfigUtil.readMultipleClientConfigAvro(getAllConfigsImpl());
        Map<String, Properties> requestedConfigs = Maps.newHashMap();

        for (String storeName: requestedStoreNames) {
            if (storeName == null || storeName.isEmpty()){
                // We ignore it...
            } else if (allConfigs.containsKey(storeName)) {
                requestedConfigs.put(storeName, allConfigs.get(storeName));
            } else {
                requestedConfigs.put(storeName, STORE_NOT_FOUND_PROPS);
            }
        }

        return ClientConfigUtil.writeMultipleClientConfigAvro(requestedConfigs);
    }
}
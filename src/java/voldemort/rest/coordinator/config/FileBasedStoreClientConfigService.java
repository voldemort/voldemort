package voldemort.rest.coordinator.config;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import voldemort.client.BootstrapFailureException;
import voldemort.common.service.ServiceType;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Stores configs on the local filesystem
 */
public class FileBasedStoreClientConfigService extends StoreClientConfigService {

    private Logger logger = Logger.getLogger(this.getClass().toString());
    private final static String PROBLEM_READING_CONFIG_FILE = "Problem reading the config file";
    private final static String PROBLEM_WRITING_CONFIG_FILE = "Problem writing the config file";

    public FileBasedStoreClientConfigService(CoordinatorConfig coordinatorConfig) {
        super(coordinatorConfig);
    }

    private File getConfigFile() {
        return new File(coordinatorConfig.getFatClientConfigPath());
    }

    private void persistNewConfigFile(Map<String, Properties> newConfigs) {
        String newConfigFileContent = ClientConfigUtil.writeMultipleClientConfigAvro(newConfigs);

        try {
            File configFile = getConfigFile();

            FileWriter fw = new FileWriter(configFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(newConfigFileContent);
            bw.close();
        } catch(IOException e) {
            logger.error(PROBLEM_WRITING_CONFIG_FILE, e);
            throw new RuntimeException(PROBLEM_WRITING_CONFIG_FILE, e);
        }
    }

    @Override
    public Map<String, Properties> getAllConfigsMap() {
        try {
            String rawConfigContent = Joiner.on("\n")
                                            .join(IOUtils.readLines(new FileReader(getConfigFile())))
                                            .trim();

            return ClientConfigUtil.readMultipleClientConfigAvro(rawConfigContent);
        } catch(IOException e) {
            logger.error(PROBLEM_READING_CONFIG_FILE, e);
            throw new RuntimeException(PROBLEM_READING_CONFIG_FILE, e);
        }
    }

    @Override
    protected Map<String, Properties> getSpecificConfigsMap(List<String> requestedStoreNames) {
        Map<String, Properties> allConfigs = getAllConfigsMap();
        Map<String, Properties> requestedConfigs = Maps.newHashMap();

        for(String storeName: requestedStoreNames) {
            if(storeName == null || storeName.isEmpty()) {
                // We ignore it...
            } else if(allConfigs.containsKey(storeName)) {
                requestedConfigs.put(storeName, allConfigs.get(storeName));
            } else {
                requestedConfigs.put(storeName, STORE_NOT_FOUND_PROPS);
            }
        }

        return requestedConfigs;
    }

    @Override
    protected Map<String, Properties> putConfigsMap(Map<String, Properties> configsToPut) {
        Map<String, Properties> allConfigs = getAllConfigsMap();
        Map<String, Properties> newConfigs = Maps.newHashMap(allConfigs);
        Map<String, Properties> response = Maps.newHashMap();

        // TODO For now assuming the only Listener is CoordinatorProxyService.
        // Later need to change the API to accept a servicetype as second
        // parameter
        StoreClientConfigServiceListener listener = this.storeClientConfigListeners.get(ServiceType.COORDINATOR_PROXY.getDisplayName());

        for(String storeNameToPut: configsToPut.keySet()) {
            if(allConfigs.containsKey(storeNameToPut)) {
                Properties existingProperties = allConfigs.get(storeNameToPut);

                if(existingProperties.equals(configsToPut.get(storeNameToPut))) {
                    response.put(storeNameToPut, STORE_UNCHANGED_PROPS);
                } else {
                    try {
                        listener.onStoreConfigAddOrUpdate(storeNameToPut,
                                                          configsToPut.get(storeNameToPut));
                    } catch(Exception e) {
                        String errorMessage = "Got exception when trying to update the fat client for store "
                                              + storeNameToPut + " - " + e.getMessage();
                        logger.error(errorMessage);
                        STORE_NOT_UPDATED_PROPS.put(ERROR_MESSAGE_PARAM_KEY, errorMessage);
                        response.put(storeNameToPut, STORE_NOT_UPDATED_PROPS);
                        continue;
                    }
                    newConfigs.put(storeNameToPut, configsToPut.get(storeNameToPut));
                    response.put(storeNameToPut, STORE_UPDATED_PROPS);
                }
            } else { // Store does not already exist
                try {
                    listener.onStoreConfigAddOrUpdate(storeNameToPut,
                                                      configsToPut.get(storeNameToPut));
                } catch(BootstrapFailureException bootstrapException) {
                    logger.error("The store " + storeNameToPut
                                 + " is not served by Voldemort currently. Exception Message - "
                                 + bootstrapException.getMessage());
                    response.put(storeNameToPut, STORE_NOT_SERVED_BY_VOLDEMORT_PROPS);
                    continue;
                } catch(Exception e) {
                    String errorMessage = "Got exception when trying to create fat client for store "
                                          + storeNameToPut
                                          + ". Exception Message - "
                                          + e.getMessage();
                    logger.error(errorMessage);
                    STORE_NOT_CREATED_PROPS.put(ERROR_MESSAGE_PARAM_KEY, errorMessage);
                    response.put(storeNameToPut, STORE_NOT_CREATED_PROPS);
                    continue;

                }
                newConfigs.put(storeNameToPut, configsToPut.get(storeNameToPut));
                response.put(storeNameToPut, STORE_CREATED_PROPS);
            }
        }

        persistNewConfigFile(newConfigs);

        return response;
    }

    @Override
    protected Map<String, Properties> deleteSpecificConfigsMap(List<String> storeNames) {
        Map<String, Properties> allConfigs = getAllConfigsMap();
        Map<String, Properties> newConfigs = Maps.newHashMap(allConfigs);
        Map<String, Properties> response = Maps.newHashMap();

        // TODO For now assuming the only Listener is CoordinatorProxyService.
        // Later need to change the API to accept a servicetype as second
        // parameter
        StoreClientConfigServiceListener listener = this.storeClientConfigListeners.get(ServiceType.COORDINATOR_PROXY.getDisplayName());

        for(String storeNameToDelete: storeNames) {
            if(allConfigs.containsKey(storeNameToDelete)) {
                try {
                    listener.onStoreConfigDelte(storeNameToDelete);
                } catch(Exception e) {
                    String errorMessage = "Got exception when trying to remove the fat client for store "
                                          + storeNameToDelete
                                          + ". Exception Message - "
                                          + e.getMessage();
                    logger.debug(errorMessage);
                    STORE_NOT_DELETED_PROPS.put(ERROR_MESSAGE_PARAM_KEY, errorMessage);
                    response.put(storeNameToDelete, STORE_NOT_DELETED_PROPS);
                    continue;
                }
                newConfigs.remove(storeNameToDelete);
                response.put(storeNameToDelete, STORE_DELETED_PROPS);
            } else { // Store does not already exist
                response.put(storeNameToDelete, STORE_ALREADY_DOES_NOT_EXIST_PROPS);
            }
        }

        persistNewConfigFile(newConfigs);

        return response;
    }
}
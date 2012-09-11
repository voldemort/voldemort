package voldemort.utils;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import voldemort.client.SystemStore;

public class MetadataVersionStoreUtils {

    public static final String VERSIONS_METADATA_KEY = "metadata-versions";
    private final static Logger logger = Logger.getLogger(MetadataVersionStoreUtils.class);

    /**
     * Retrieves a properties (hashmap) consisting of all the metadata versions
     * 
     * @param versionStore The system store client used to retrieve the metadata
     *        versions
     * @return Properties object containing all the
     *         'property_name=property_value' values
     */
    public static Properties getProperties(SystemStore<String, String> versionStore) {
        Properties props = null;
        try {
            String versionList = versionStore.getSysStore(VERSIONS_METADATA_KEY).getValue();

            if(versionList != null) {
                props = new Properties();
                props.load(new ByteArrayInputStream(versionList.getBytes()));
            }
        } catch(Exception e) {
            logger.debug("Got exception in getting properties : " + e.getMessage());
        }

        return props;
    }

    /**
     * Writes the Properties object to the Version metadata system store
     * 
     * @param versionStore The system store client used to retrieve the metadata
     *        versions
     * @param props The Properties object to write to the System store
     */
    public static void setProperties(SystemStore<String, String> versionStore, Properties props) {
        if(props == null) {
            return;
        }

        try {
            StringBuilder finalVersionList = new StringBuilder();
            for(String propName: props.stringPropertyNames()) {
                if(finalVersionList.length() == 0) {
                    finalVersionList.append(propName + "=" + props.getProperty(propName));
                } else {
                    finalVersionList.append("\n" + propName + "=" + props.getProperty(propName));
                }
            }
            versionStore.putSysStore(VERSIONS_METADATA_KEY, finalVersionList.toString());
        } catch(Exception e) {
            logger.debug("Got exception in setting properties : " + e.getMessage());
        }
    }
}

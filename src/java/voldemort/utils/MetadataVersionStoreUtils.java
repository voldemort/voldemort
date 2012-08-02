package voldemort.utils;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import voldemort.client.SystemStore;

public class MetadataVersionStoreUtils {

    public static final String VERSIONS_METADATA_STORE = "metadata-versions";
    private final static Logger logger = Logger.getLogger(MetadataVersionStoreUtils.class);

    public static Properties getProperties(SystemStore<String, String> versionStore) {
        Properties props = null;
        try {
            String versionList = versionStore.getSysStore(VERSIONS_METADATA_STORE).getValue();

            if(versionList != null) {
                props = new Properties();
                props.load(new ByteArrayInputStream(versionList.getBytes()));
            }
        } catch(Exception e) {
            logger.debug("Got exception in getting properties : " + e.getMessage());
        }

        return props;
    }

    public static void setProperties(SystemStore<String, String> versionStore, Properties props) {
        StringBuilder finalVersionList = new StringBuilder();
        for(String propName: props.stringPropertyNames()) {
            if(finalVersionList.length() == 0) {
                finalVersionList.append(propName + "=" + props.getProperty(propName));
            } else {
                finalVersionList.append("\n" + propName + "=" + props.getProperty(propName));
            }
        }

        System.err.println(finalVersionList);

        versionStore.putSysStore(VERSIONS_METADATA_STORE, finalVersionList.toString());
    }
}

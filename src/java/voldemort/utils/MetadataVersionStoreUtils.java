/*
 * Copyright 2008-2012 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.utils;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import voldemort.client.SystemStoreClient;
import voldemort.store.system.SystemStoreConstants;
import voldemort.versioning.Versioned;

/**
 * A Utils class that facilitates conversion between the string containing
 * metadata versions and the corresponding Properties object.
 *
 * @author csoman
 *
 */
public class MetadataVersionStoreUtils {

    private final static Logger logger = Logger.getLogger(MetadataVersionStoreUtils.class);

    /**
     * Retrieves a properties (hashmap) consisting of all the metadata versions
     *
     * @param versionStore The system store client used to retrieve the metadata
     *        versions
     * @return Properties object containing all the
     *         'property_name=property_value' values
     */
    public static Properties getProperties(SystemStoreClient<String, String> versionStore) {
        Properties props = null;
        Versioned<String> versioned = versionStore.getSysStore(SystemStoreConstants.VERSIONS_METADATA_KEY);
        if(versioned != null && versioned.getValue() != null) {
            try {
                String versionList = versioned.getValue();
                props = new Properties();
                props.load(new ByteArrayInputStream(versionList.getBytes()));
            } catch(Exception e) {
                logger.debug("Got exception in getting properties : " + e.getMessage());
                e.printStackTrace();
            }
        }

        return props;
    }

    private static String getPropertiesString(Properties props) {
        StringBuilder finalVersionList = new StringBuilder();
        for(String propName: props.stringPropertyNames()) {
            if(finalVersionList.length() == 0) {
                finalVersionList.append(propName + "=" + props.getProperty(propName));
            } else {
                finalVersionList.append("\n" + propName + "=" + props.getProperty(propName));
            }
        }

        return finalVersionList.toString();
    }

    /**
     * Writes the Properties object to the Version metadata system store
     * 
     * @param versionStore The system store client used to retrieve the metadata
     *        versions
     * @param props The Properties object to write to the System store
     */
    public static void setProperties(SystemStoreClient<String, String> versionStore,
                                     Versioned<Properties> props) {
        if(props == null || props.getValue() == null) {
            return;
        }

        try {
            String versionString = getPropertiesString(props.getValue());
            Versioned<String> versionedString = new Versioned<String>(versionString,
                                                                      props.getVersion());
            versionStore.putSysStore(SystemStoreConstants.VERSIONS_METADATA_KEY, versionedString);
        } catch(Exception e) {
            logger.debug("Got exception in setting properties : " + e.getMessage());
        }
    }

    /**
     * Writes the Properties object to the Version metadata system store
     *
     * @param versionStore The system store client used to retrieve the metadata
     *        versions
     * @param props The Properties object to write to the System store
     */
    public static void setProperties(SystemStoreClient<String, String> versionStore,
                                     Properties props) {
        if(props == null) {
            return;
        }

        try {
            String versionString = getPropertiesString(props);
            versionStore.putSysStore(SystemStoreConstants.VERSIONS_METADATA_KEY,
                                     versionString);
        } catch(Exception e) {
            logger.debug("Got exception in setting properties : " + e.getMessage());
        }
    }
}

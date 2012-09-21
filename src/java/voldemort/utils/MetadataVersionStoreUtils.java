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

import voldemort.client.SystemStore;

/**
 * A Utils class that facilitates conversion between the string containing
 * metadata versions and the corresponding Properties object.
 * 
 * @author csoman
 * 
 */
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

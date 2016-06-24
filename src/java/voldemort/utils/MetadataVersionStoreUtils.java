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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import voldemort.client.SystemStoreClient;
import voldemort.store.system.SystemStoreConstants;
import voldemort.versioning.VectorClock;
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
                logger.warn("Error retrieving properties ", e);
            }
        }

        return props;
    }

    private static long tryParse(String s) {
        try {
            return Long.parseLong(s);
        } catch(NumberFormatException e) {
            return 0L;
        }
    }

    public static Long getVersion(Properties prop, String versionKey) {
        long value = 0;

        if(prop != null && prop.getProperty(versionKey) != null) {
            String strValue = prop.getProperty(versionKey);
            value = tryParse(strValue);
        }

        if(logger.isDebugEnabled()) {
            logger.debug("*********** For key : " + versionKey + " received value = " + value);
        }
        return value;
    }

    public static Properties mergeVersions(Properties prop1, Properties prop2) {
        if(prop1 == null) {
            return prop2;
        }
        if(prop2 == null) {
            return prop1;
        }

        Properties result = new Properties();

        for(String propName: prop1.stringPropertyNames()) {
            result.setProperty(propName, prop1.getProperty(propName));
        }

        for(String propName: prop2.stringPropertyNames()) {
            String currValue = result.getProperty(propName);
            long currlValue = tryParse(currValue);

            String newValue = prop2.getProperty(propName);
            long lValue = tryParse(newValue);
            
            long maxlValue = Math.max(currlValue, lValue);
            result.setProperty(propName, Long.toString(maxlValue));
        }
        return result;
    }

    public static Versioned<Properties> parseProperties(List<Versioned<byte[]>> versionProps) {
        Properties props = new Properties();
        if(versionProps == null || versionProps.size() == 0) {
            return new Versioned<Properties>(props);
        }

        VectorClock version = new VectorClock();
        for(int cur = 0; cur < versionProps.size(); cur++) {
            VectorClock curVersion = (VectorClock) versionProps.get(cur).getVersion();
            version = version.merge(curVersion);

            byte[] value = versionProps.get(cur).getValue();
            if(value == null) {
                continue;
            }

            Properties curProps = new Properties();
            try {
                curProps.load(new ByteArrayInputStream(value));
            } catch(Exception e) {
                continue;
            }
            
            props = mergeVersions(props, curProps);
        }

        return new Versioned<Properties>(props, version);
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

    public static byte[] convertToByteArray(Properties props) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        props.store(out, null);
        out.flush();
        return out.toByteArray();
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
            logger.info("Ignoring set for empty properties");
            return;
        }

        try {
            String versionString = getPropertiesString(props.getValue());
            Versioned<String> versionedString = new Versioned<String>(versionString,
                                                                      props.getVersion());
            versionStore.putSysStore(SystemStoreConstants.VERSIONS_METADATA_KEY, versionedString);
        } catch(Exception e) {
            logger.info("Got exception in setting properties : ", e);
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
        if (props == null) {
            logger.info("Ignoring set for empty properties");
            return;
        }

        try {
            String versionString = getPropertiesString(props);
            versionStore.putSysStore(SystemStoreConstants.VERSIONS_METADATA_KEY,
                                     versionString);
        } catch(Exception e) {
            logger.info("Got exception in setting properties : ", e);
        }
    }
}

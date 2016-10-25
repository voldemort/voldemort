/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.readonly.mr.utils;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.List;

import org.apache.log4j.Logger;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class VoldemortUtils {

    private static final Logger logger = Logger.getLogger(VoldemortUtils.class);

    /**
     * Given the comma separated list of properties as a string, splits it
     * multiple strings
     * 
     * @param paramValue Concatenated string
     * @param type Type of parameter ( to throw exception )
     * @return List of string properties
     */
    public static List<String> getCommaSeparatedStringValues(String paramValue, String type) {
        List<String> commaSeparatedProps = Lists.newArrayList();
        for(String url: Utils.COMMA_SEP.split(paramValue.trim()))
            if(url.trim().length() > 0)
                commaSeparatedProps.add(url);

        if(commaSeparatedProps.size() == 0) {
            throw new RuntimeException("Number of " + type + " should be greater than zero");
        }
        return commaSeparatedProps;
    }

    public static String getStoreDefXml(String storeName,
                                        int replicationFactor,
                                        int requiredReads,
                                        int requiredWrites,
                                        Integer preferredReads,
                                        Integer preferredWrites,
                                        String keySerializer,
                                        String valSerializer,
                                        String description,
                                        String owners) {
        StringBuffer storeXml = new StringBuffer();

        storeXml.append("<store>\n\t<name>");
        storeXml.append(storeName);
        storeXml.append("</name>\n\t<persistence>read-only</persistence>\n\t");
        if(description.length() != 0) {
            storeXml.append("<description>");
            storeXml.append(description);
            storeXml.append("</description>\n\t");
        }
        if(owners.length() != 0) {
            storeXml.append("<owners>");
            storeXml.append(owners);
            storeXml.append("</owners>\n\t");
        }
        storeXml.append("<routing>client</routing>\n\t<replication-factor>");
        storeXml.append(replicationFactor);
        storeXml.append("</replication-factor>\n\t<required-reads>");
        storeXml.append(requiredReads);
        storeXml.append("</required-reads>\n\t<required-writes>");
        storeXml.append(requiredWrites);
        storeXml.append("</required-writes>\n\t");
        if(preferredReads != null)
            storeXml.append("<preferred-reads>")
                    .append(preferredReads)
                    .append("</preferred-reads>\n\t");
        if(preferredWrites != null)
            storeXml.append("<preferred-writes>")
                    .append(preferredWrites)
                    .append("</preferred-writes>\n\t");
        storeXml.append("<key-serializer>");
        storeXml.append(keySerializer);
        storeXml.append("</key-serializer>\n\t<value-serializer>");
        storeXml.append(valSerializer);
        storeXml.append("</value-serializer>\n</store>");

        return storeXml.toString();
    }

    public static String getStoreDefXml(String storeName,
                                        int replicationFactor,
                                        int requiredReads,
                                        int requiredWrites,
                                        Integer preferredReads,
                                        Integer preferredWrites,
                                        String keySerializer,
                                        String valSerializer) {
        StringBuffer storeXml = new StringBuffer();

        storeXml.append("<store>\n\t<name>");
        storeXml.append(storeName);
        storeXml.append("</name>\n\t<persistence>read-only</persistence>\n\t<routing>client</routing>\n\t<replication-factor>");
        storeXml.append(replicationFactor);
        storeXml.append("</replication-factor>\n\t<required-reads>");
        storeXml.append(requiredReads);
        storeXml.append("</required-reads>\n\t<required-writes>");
        storeXml.append(requiredWrites);
        storeXml.append("</required-writes>\n\t");
        if(preferredReads != null)
            storeXml.append("<preferred-reads>")
                    .append(preferredReads)
                    .append("</preferred-reads>\n\t");
        if(preferredWrites != null)
            storeXml.append("<preferred-writes>")
                    .append(preferredWrites)
                    .append("</preferred-writes>\n\t");
        storeXml.append("<key-serializer>");
        storeXml.append(keySerializer);
        storeXml.append("</key-serializer>\n\t<value-serializer>");
        storeXml.append(valSerializer);
        storeXml.append("</value-serializer>\n</store>");

        return storeXml.toString();
    }

    public static StoreDefinition getStoreDef(String xml) {
        return new StoreDefinitionsMapper().readStore(new StringReader(xml));
    }

    public static String modifyURL(String originalUrl, VoldemortConfig config) {
        return modifyURL(originalUrl,
                         config.getReadOnlyModifyProtocol(),
                         config.getReadOnlyModifyPort(),
                         config.getReadOnlyOmitPort());
    }

    public static String modifyURL(String originalUrl, String newProtocol, int newPort, boolean omitPort) {
        if (newProtocol == null || newProtocol.isEmpty() || (!omitPort && newPort < 0)) {
            return originalUrl;
        }

        try {
            // Create handler to avoid unknown protocol error when parsing URL string. Actually this handler will do
            // nothing.
            URLStreamHandler handler = new URLStreamHandler() {
                @Override
                protected URLConnection openConnection(URL u)
                    throws IOException {
                    return null;
                }
            };

            URL url = new URL(null, originalUrl, handler);
            logger.info("Existing protocol = " + url.getProtocol() + " and port = " + url.getPort());

            if (omitPort) {
                // The URL constructor will omit the port if we pass -1 in the port.
                newPort = -1;
            }

            URL newUrl = new URL(newProtocol, url.getHost(), newPort, url.getFile(), handler);
            logger.info("New protocol = " + newUrl.getProtocol() + " and port = " + newUrl.getPort());
            return newUrl.toString();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("URL is not in valid format. URL:" + originalUrl);
        }
    }
}

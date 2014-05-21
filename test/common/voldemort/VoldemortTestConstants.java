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

package voldemort;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.IOUtils;

import voldemort.cluster.Cluster;
import voldemort.xml.ClusterMapper;

public class VoldemortTestConstants {

    public static String getOneNodeClusterXml() {
        return readString("config/one-node-cluster.xml");
    }

    public static Cluster getOneNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getOneNodeClusterXml()));
    }

    public static String getSimpleStoreDefinitionsXml() {
        return readString("config/stores.xml");
    }

    public static String getSingleStoreDefinitionsXml() {
        return readString("config/single-store.xml");
    }

    public static String getTwoStoreDefinitionsXml() {
        return readString("config/two-stores.xml");
    }

    public static String getNoVersionStoreDefinitionsXml() {
        return readString("config/no-version-store.xml");
    }

    public static String getStoreDefinitionsWithRetentionXml() {
        return readString("config/store-with-retention.xml");
    }

    public static String getTwoNodeClusterXml() {
        return readString("config/two-node-cluster.xml");
    }

    public static String getThreeNodeClusterWith3ZonesXml() {
        return readString("config/three-node-cluster-with-3-zones.xml");
    }

    public static String getStoreWithTwoKeyVersions() {
        return readString("config/store-with-two-key-versions.xml");
    }

    public static Cluster getTwoNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getTwoNodeClusterXml()));
    }

    public static Cluster getThreeNodeClusterWith3Zones() {
        return new ClusterMapper().readCluster(new StringReader(getThreeNodeClusterWith3ZonesXml()));
    }

    public static String getSixNodeClusterWith3ZonesXml() {
        return readString("config/six-node-cluster-with-3-zones.xml");
    }

    public static Cluster getSixNodeClusterWith3Zones() {
        return new ClusterMapper().readCluster(new StringReader(getSixNodeClusterWith3ZonesXml()));
    }

    public static String getTenNodeClusterXml() {
        return readString("config/ten-node-cluster.xml");
    }

    public static String getNineNodeClusterXml() {
        return readString("config/nine-node-cluster.xml");
    }

    public static String getThreeNodeClusterXml() {
        return readString("config/three-node-cluster.xml");
    }

    public static String getFourNodeClusterWithZonesXml() {
        return readString("config/four-node-cluster-with-zones.xml");
    }

    public static String getEightNodeClusterWithZonesXml() {
        return readString("config/eight-node-cluster-with-zones.xml");
    }

    public static String getNineNodeClusterWith3ZonesXml() {
        return readString("config/nine-node-cluster-with-3-zones.xml");
    }

    public static String getSingleStoreWithZonesXml() {
        return readString("config/single-store-with-zones.xml");
    }

    public static String getTwoStoresWithZonesXml() {
        return readString("config/two-stores-with-zones.xml");
    }

    public static String getThreeStoresWithZonesXml() {
        return readString("config/three-stores-with-zones.xml");
    }

    public static Cluster getTenNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getTenNodeClusterXml()));
    }

    public static Cluster getNineNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getNineNodeClusterXml()));
    }

    public static Cluster getThreeNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getThreeNodeClusterXml()));
    }

    public static Cluster getFourNodeClusterWithZones() {
        return new ClusterMapper().readCluster(new StringReader(getFourNodeClusterWithZonesXml()));
    }

    public static Cluster getEightNodeClusterWithZones() {
        return new ClusterMapper().readCluster(new StringReader(getEightNodeClusterWithZonesXml()));
    }

    public static Cluster getNineNodeClusterWith3Zones() {
        return new ClusterMapper().readCluster(new StringReader(getNineNodeClusterWith3ZonesXml()));
    }

    private static String readString(String filename) {
        try {
            InputStream inputStream = VoldemortTestConstants.class.getResourceAsStream(filename);
            if(inputStream == null) {
                throw new VoldemortException("Could not locate resource " + filename);
            }
            return IOUtils.toString(inputStream);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getCompressedStoreDefinitionsXml() {
        return readString("config/compressed-store.xml");
    }

    public static String getViewStoreDefinitionXml() {
        return readString("config/view-store.xml");
    }

    public static String getSingleStore322Xml() {
        return readString("config/single-store-322.xml");
    }

}

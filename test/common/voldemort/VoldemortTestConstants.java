package voldemort;

import java.io.IOException;
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

    public static String getStoreDefinitionsWithRetentionXml() {
        return readString("config/store-with-retention.xml");
    }

    public static String getTwoNodeClusterXml() {
        return readString("config/two-node-cluster.xml");
    }

    public static Cluster getTwoNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getTwoNodeClusterXml()));
    }

    public static String getNineNodeClusterXml() {
        return readString("config/nine-node-cluster.xml");
    }

    public static Cluster getNineNodeCluster() {
        return new ClusterMapper().readCluster(new StringReader(getNineNodeClusterXml()));
    }

    private static String readString(String filename) {
        try {
            return IOUtils.toString(VoldemortTestConstants.class.getResourceAsStream(filename));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

}

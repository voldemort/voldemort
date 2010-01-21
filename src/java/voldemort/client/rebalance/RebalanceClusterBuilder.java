package voldemort.client.rebalance;

import java.io.IOException;

/**
 * @author afeinberg
 */
public class RebalanceClusterBuilder {
    
    private final String clusterXmlFile;
    private final String storesXmlFile;
    
    public RebalanceClusterBuilder(String clusterXmlFile, String storesXmlFile) {
        this.clusterXmlFile = clusterXmlFile;
        this.storesXmlFile = storesXmlFile;
    }

    public void build(String targetClusterXml,
                      String host,
                      String httpPort,
                      int socketPort,
                      int adminPort) throws IOException {

    }

    public static void main(String [] args) throws IOException {

    }
}

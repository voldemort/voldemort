package voldemort.coordinator;

import java.util.List;

import voldemort.store.StoreDefinition;

public class CoordinatorMetadata {

    private String clusterXmlStr;
    private List<StoreDefinition> storeDefList;

    public CoordinatorMetadata() {
        this.clusterXmlStr = null;
        this.storeDefList = null;
    }

    public CoordinatorMetadata(String clusterXml, List<StoreDefinition> storeDefList) {
        this.setMetadata(clusterXml, storeDefList);
    }

    public synchronized String getClusterXmlStr() {
        return clusterXmlStr;
    }

    public synchronized List<StoreDefinition> getStoresDefs() {
        return this.storeDefList;
    }

    public synchronized void setMetadata(String clusterXml, List<StoreDefinition> storeDefList) {
        this.clusterXmlStr = clusterXml;
        this.storeDefList = storeDefList;
    }

}

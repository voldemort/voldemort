package voldemort.rest.coordinator;

import java.io.StringReader;
import java.util.List;

import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

public class CoordinatorMetadata {

    private String clusterXmlStr, storesXmlStr;
    private List<StoreDefinition> storeDefList;
    StoreDefinitionsMapper storeMapper;

    public CoordinatorMetadata() {
        storeMapper = new StoreDefinitionsMapper();
        this.storesXmlStr = null;
        this.clusterXmlStr = null;
        this.storeDefList = null;
    }

    public CoordinatorMetadata(String clusterXml, String storesXml) {
        storeMapper = new StoreDefinitionsMapper();
        this.setMetadata(clusterXml, storesXml);
    }

    public synchronized String getClusterXmlStr() {
        return clusterXmlStr;
    }

    public synchronized List<StoreDefinition> getStoresDefs() {
        return this.storeDefList;
    }

    public synchronized String getStoreDefs() {
        return this.storesXmlStr;
    }

    public synchronized void setMetadata(String clusterXml, String storesXml) {
        this.storesXmlStr = storesXml;
        this.clusterXmlStr = clusterXml;
        this.storeDefList = storeMapper.readStoreList(new StringReader(storesXmlStr), false);
    }
}

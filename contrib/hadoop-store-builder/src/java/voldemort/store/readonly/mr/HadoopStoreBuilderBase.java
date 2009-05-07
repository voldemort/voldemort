package voldemort.store.readonly.mr;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class HadoopStoreBuilderBase {

    private Cluster cluster;
    private StoreDefinition storeDef;

    public void configure(JobConf conf) {
        this.cluster = new ClusterMapper().readCluster(new StringReader(conf.get("cluster.xml")));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));
        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");
        this.storeDef = storeDefs.get(0);
    }

    @SuppressWarnings("unused")
    public void close() throws IOException {}

    public Cluster getCluster() {
        checkNotNull(cluster);
        return cluster;
    }

    public StoreDefinition getStoreDef() {
        checkNotNull(storeDef);
        return storeDef;
    }

    public String getStoreName() {
        checkNotNull(storeDef);
        return storeDef.getName();
    }

    private final void checkNotNull(Object o) {
        if(o == null)
            throw new VoldemortException("Not configured yet!");
    }

}

package voldemort.client.rebalance;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class RebalanceCommandShell {

    private static final String PROMPT = "> ";

    private static RebalanceClient rebalanceClient;
    private static ClusterMapper clusterMapper = new ClusterMapper();
    private static StoreDefinitionsMapper storesMapper = new StoreDefinitionsMapper();

    public static void main(String[] args) throws Exception {
        if(args.length < 4 || args.length > 5)
            Utils.croak("USAGE: java RebalanceCommandShell currentCluster.xml targetCluster.xml stores.xml maxParallelRebalancing");

        Cluster currentCluster = clusterMapper.readCluster(new File(args[0]));
        Cluster targetCluster = clusterMapper.readCluster(new File(args[1]));
        List<StoreDefinition> storesList = storesMapper.readStoreList(new File(args[2]));
        int maxParallellRebalancing = Integer.parseInt(args[3]);

        rebalanceClient = new RebalanceClient(currentCluster,
                                              maxParallellRebalancing,
                                              new AdminClientConfig());

        List<String> storeNames = new ArrayList<String>(storesList.size());
        for(StoreDefinition storeDef: storesList)
            storeNames.add(storeDef.getName());

        rebalanceClient.rebalance(targetCluster, storeNames);
    }
}
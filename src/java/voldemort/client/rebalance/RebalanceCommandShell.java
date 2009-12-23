package voldemort.client.rebalance;

import java.io.File;

import voldemort.cluster.Cluster;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class RebalanceCommandShell {

    private static final String PROMPT = "> ";

    private static RebalanceClient rebalanceClient;
    private static ClusterMapper clusterMapper = new ClusterMapper();
    private static StoreDefinitionsMapper storesMapper = new StoreDefinitionsMapper();

    public static void main(String[] args) throws Exception {
        if(args.length != 4)
            Utils.croak("USAGE: java RebalanceCommandShell bootstrapURL currentCluster.xml targetCluster.xml maxParallelRebalancing");

        String bootstrapURL = args[0];
        Cluster currentCluster = clusterMapper.readCluster(new File(args[1]));
        Cluster targetCluster = clusterMapper.readCluster(new File(args[2]));
        int maxParallelRebalancing = Integer.parseInt(args[3]);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(maxParallelRebalancing);

        rebalanceClient = new RebalanceClient(bootstrapURL, config);

        rebalanceClient.rebalance(currentCluster, targetCluster);
    }
}
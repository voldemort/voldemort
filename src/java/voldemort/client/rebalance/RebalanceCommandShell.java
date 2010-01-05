package voldemort.client.rebalance;

import java.io.File;

import voldemort.cluster.Cluster;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

public class RebalanceCommandShell {

    private static RebalanceController rebalanceClient;
    private static ClusterMapper clusterMapper = new ClusterMapper();

    public static void main(String[] args) throws Exception {
        if(args.length != 3)
            Utils.croak("USAGE: java RebalanceCommandShell bootstrapURL targetCluster.xml maxParallelRebalancing");

        String bootstrapURL = args[0];
        Cluster targetCluster = clusterMapper.readCluster(new File(args[1]));
        int maxParallelRebalancing = Integer.parseInt(args[2]);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(maxParallelRebalancing);

        rebalanceClient = new RebalanceController(bootstrapURL, config);

        rebalanceClient.rebalance(targetCluster);
    }
}
package voldemort.client.rebalance;

import java.io.File;

import voldemort.cluster.Cluster;
import voldemort.xml.ClusterMapper;

public class RebalanceCommandShell {

    private static RebalanceController rebalanceClient;
    private static ClusterMapper clusterMapper = new ClusterMapper();

    private static void printUsage() {
        StringBuilder builder = new StringBuilder();
        builder.append("java RebalanceCommandShell bootstrapURL targetCluster.xml maxParallelRebalancing deleteAfterRebalancing\n");
        builder.append("Arguments:\n");
        builder.append("bootstrapUrl: bootstrap voldemort server url.(should point to a node in the cluster and not the new nodes)\n");
        builder.append("targetCluster.xml: The final desired cluster configuration.\n");
        builder.append("maxParallelRebalancing: maximum parallel transfers to start\n");
        builder.append("deleteAfterRebalancing: delete data from original nodes after transfering.\n");

        System.out.println(builder.toString());
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 4)
            printUsage();

        String bootstrapURL = args[0];
        Cluster targetCluster = clusterMapper.readCluster(new File(args[1]));
        int maxParallelRebalancing = Integer.parseInt(args[2]);

        if(!"true".equals(args[3]) && !"false".equals(args[3])) {
            printUsage();
        }

        boolean deleteAfterRebalancing = Boolean.parseBoolean(args[3]);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(maxParallelRebalancing);
        config.setDeleteAfterRebalancingEnabled(deleteAfterRebalancing);

        rebalanceClient = new RebalanceController(bootstrapURL, config);

        rebalanceClient.rebalance(targetCluster);
        rebalanceClient.stop();
    }
}
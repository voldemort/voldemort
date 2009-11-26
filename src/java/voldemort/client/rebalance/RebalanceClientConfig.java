package voldemort.client.rebalance;

import java.util.Properties;

import voldemort.client.ClientConfig;
import voldemort.utils.Props;

/**
 * Client Configuration properties for Rebalance client. extends
 * {@link ClientConfig}
 * 
 * @author bbansal
 * 
 */
public class RebalanceClientConfig extends ClientConfig {

    // Rebalance client configurations
    private static final String CLUSTER_MAJORITY_THRESHOLD_PERCENTAGE = "cluster.majority.threshold.percentage";
    private static final String MAX_PARALLEL_REBALANCING_NODES = "max.parallel.rebalancing.node";

    // Rebalance properties
    private double clusterMajorityThresholdPercentage = 0.75;
    private int maxParallelRebalancingNodes = 1;

    public RebalanceClientConfig() {}

    public RebalanceClientConfig(Properties properties) {
        super(properties);

        Props props = new Props(properties);

        if(properties.containsKey(CLUSTER_MAJORITY_THRESHOLD_PERCENTAGE))
            this.setClusterMajorityThresholdPercentage(props.getDouble(CLUSTER_MAJORITY_THRESHOLD_PERCENTAGE));

        if(properties.containsKey(MAX_PARALLEL_REBALANCING_NODES))
            this.setMaxParallelRebalancingNodes(props.getInt(MAX_PARALLEL_REBALANCING_NODES));
    }

    public void setClusterMajorityThresholdPercentage(double clusterMajorityThresholdPercentage) {
        this.clusterMajorityThresholdPercentage = clusterMajorityThresholdPercentage;
    }

    public double getClusterMajorityThresholdPercentage() {
        return clusterMajorityThresholdPercentage;
    }

    public void setMaxParallelRebalancingNodes(int maxParallelRebalancingNodes) {
        this.maxParallelRebalancingNodes = maxParallelRebalancingNodes;
    }

    public int getMaxParallelRebalancingNodes() {
        return maxParallelRebalancingNodes;
    }
}

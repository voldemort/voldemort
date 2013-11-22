package voldemort.partition;

import java.io.File;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

public class TestDistribution {

    public static void main(String[] args) throws Exception {
        if(args.length < 2)
            Utils.croak("USAGE: java TestDistribution cluster.xml replication_factor max_id");
        long start = System.currentTimeMillis();
        File file = new File(args[0]);
        int repFactor = Integer.parseInt(args[1]);
        int maxVal = Integer.parseInt(args[2]);
        ClusterMapper mapper = new ClusterMapper();
        Cluster cluster = mapper.readCluster(file);
        RoutingStrategy strategy = new ConsistentRoutingStrategy(cluster, repFactor);
        JsonTypeSerializer serializer = new JsonTypeSerializer(JsonTypeDefinition.INT32);
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

        for(int i = 0; i < maxVal; i++) {
            for(Node node: strategy.routeRequest(serializer.toBytes(i))) {
                int newCount = 1;
                if(counts.get(node.getId()) != null) {
                    newCount = counts.get(node.getId()) + 1;
                }
                counts.put(node.getId(), newCount);
            }
        }

        int sum = 0;
        int totalCounts = 0;
        for(int countVal: counts.values()) {
            sum += countVal;
            totalCounts++;
        }
        int avg = sum / totalCounts;

        NumberFormat percent = NumberFormat.getPercentInstance();
        percent.setMaximumFractionDigits(2);
        System.out.println("Node\tKeys\tPercent\tVariation");

        Integer[] sortedNodes = (Integer[]) counts.keySet().toArray();

        for(int nodeId: sortedNodes) {
            System.out.println(nodeId
                               + "\t"
                               + counts.get(nodeId)
                               + "\t"
                               + percent.format(counts.get(nodeId) / (double) sum)
                               + "\t"
                               + percent.format((counts.get(nodeId) - avg)
                                                / (double) counts.get(nodeId)));
        }

        double msPerHash = (System.currentTimeMillis() - start) / ((double) repFactor * maxVal);
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(10);
        System.out.println(nf.format(msPerHash) + " ms per hash");
    }
}

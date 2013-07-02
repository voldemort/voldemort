package voldemort.partition;

import java.io.File;
import java.text.NumberFormat;

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
        int[] counts = new int[cluster.getNumberOfNodes()];
        for(int i = 0; i < maxVal; i++) {
            for(Node node: strategy.routeRequest(serializer.toBytes(i)))
                counts[node.getId()] += 1;
        }

        int sum = 0;
        for(int i = 0; i < counts.length; i++)
            sum += counts[i];
        int avg = sum / counts.length;

        NumberFormat percent = NumberFormat.getPercentInstance();
        percent.setMaximumFractionDigits(2);
        System.out.println("Node\tKeys\tPercent\tVariation");
        for(int i = 0; i < counts.length; i++)
            System.out.println(i + "\t" + counts[i] + "\t"
                               + percent.format(counts[i] / (double) sum) + "\t"
                               + percent.format((counts[i] - avg) / (double) counts[i]));

        double msPerHash = (System.currentTimeMillis() - start) / ((double) repFactor * maxVal);
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(10);
        System.out.println(nf.format(msPerHash) + " ms per hash");
    }
}

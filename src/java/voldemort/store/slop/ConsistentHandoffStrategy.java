package voldemort.store.slop;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ConsistentHandoffStrategy implements HintedHandoffStrategy {

    private final Random random;

    private final Map<Integer, List<Node>> routeToMap;

    public ConsistentHandoffStrategy(Cluster cluster, int prefListSize) {
        int nodesInCluster = cluster.getNumberOfNodes();
        if(prefListSize > nodesInCluster - 1)
            throw new IllegalArgumentException("Preference list size must be less than " +
                                               "number of nodes in the cluster - 1");
        
        this.random = new Random();
        this.routeToMap = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());

        for(Node node: cluster.getNodes()) {
            List<Node> prefList = Lists.newArrayListWithCapacity(prefListSize);
            int i = node.getId();
            int n = 0;
            while(n < prefListSize) {
                i = (i + 1) % cluster.getNumberOfNodes();
                if(i != node.getId()) {
                    prefList.add(cluster.getNodeById(i));
                    n++;
                }
            }
            routeToMap.put(i, prefList);
        }
    }

    public List<Node> routeHint(Node origin) {
        List<Node> prefList = Lists.newArrayList(routeToMap.get(origin.getId()));
        Collections.shuffle(prefList, random);
        return prefList;
    }
}

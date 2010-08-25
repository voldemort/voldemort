package voldemort.store.slop;

import com.google.common.collect.Lists;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class HandoffToAllStrategy implements HintedHandoffStrategy {

    private final Random random;

    private final List<Node> nodes;

    public HandoffToAllStrategy(Cluster cluster) {
        nodes = Lists.newArrayList(cluster.getNodes());
        random = new Random();
    }

    public List<Node> routeHint(Node origin) {
        Collections.shuffle(nodes, random);
        List<Node> prefList = Lists.newArrayListWithCapacity(nodes.size());
        for(Node node: nodes) {
            if(node.getId() != origin.getId())
                prefList.add(node);
        }
        return prefList;
    }
}

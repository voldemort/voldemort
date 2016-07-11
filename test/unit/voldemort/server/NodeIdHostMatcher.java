package voldemort.server;

import voldemort.cluster.Node;


public class NodeIdHostMatcher extends HostMatcher {

    private final int nodeId;
    public NodeIdHostMatcher(int nodeId) {
        super();
        this.nodeId = nodeId;
    }

    @Override
    public boolean match(Node node) {
        return node.getId() == nodeId;
    }

}

package voldemort.server;

import java.util.Arrays;

import voldemort.cluster.Node;


public class NodeIdHostMatcher extends PosixHostMatcher {

    private final int nodeId;
    public NodeIdHostMatcher(int nodeId) {
        super(Arrays.asList(HostMatcher.FQDN, HostMatcher.HOSTNAME));
        this.nodeId = nodeId;
    }

    @Override
    public boolean match(Node node) {
        return node.getId() == nodeId;
    }

}

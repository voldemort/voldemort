package voldemort.server;

import java.util.List;

import voldemort.cluster.Node;


public class PosixHostMatcher extends HostMatcher {

    public PosixHostMatcher(List<String> types) {
        super(types);
    }

    @Override
    protected boolean match(Node node, String host) {
        return node.getHost().equals(host);
    }

}

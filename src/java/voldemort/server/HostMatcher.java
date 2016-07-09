package voldemort.server;

import java.net.InetAddress;
import java.net.UnknownHostException;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;


public class HostMatcher {

    public boolean match(Node node) {
        String host = getHost();
        return match(node, host);
    }

    protected boolean match(Node node, String host) {
        return node.getHost().equalsIgnoreCase(host);
    }

    protected String getHost() {
        return Host.getFQDN();
    }

    public boolean isLocalAddress(Node node) {
        String host = node.getHost();
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch(UnknownHostException ex) {
            throw new VoldemortApplicationException("Error retrieving InetAddress for host " + host,
                                                    ex);
        }
        return hostAddress.isAnyLocalAddress() || hostAddress.isLoopbackAddress();
    }

    public String getDebugInfo() {
        return "FQDN : " + getHost();
    }

}

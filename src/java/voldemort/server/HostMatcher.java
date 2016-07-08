package voldemort.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.utils.ReflectUtils;


public abstract class HostMatcher {

    private final List<String> types;
    public static final String HOSTNAME = "hostname";
    public static final String FQDN = "fqdn";

    public HostMatcher(List<String> types) {
        this.types = types;
    }

    public boolean match(Node node) {
        for(String type: types) {
            String host = getHost(type);
            if(match(node, host)) {
                return true;
            }
        }
        return false;
    }

    abstract protected boolean match(Node node, String host);

    protected String getHost(String type) {
        if(type.equalsIgnoreCase(HOSTNAME)) {
            return Host.getName();
        } else if(type.equalsIgnoreCase(FQDN)) {
            return Host.getFQDN();
        } else {
            throw new VoldemortApplicationException("Unrecognized type" + type);
        }
    }

    public static HostMatcher getImplementation(String className, List<String> types) {
        if(className == PosixHostMatcher.class.getName()) {
            return new PosixHostMatcher(types);
        } else {
            Class<?> factoryClass = ReflectUtils.loadClass(className.trim());
            return (HostMatcher) ReflectUtils.callConstructor(factoryClass,
                                                              new Class<?>[] { types.getClass() },
                                                              new Object[] { types });
        }
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
}

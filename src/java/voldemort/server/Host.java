package voldemort.server;

import java.net.InetAddress;
import java.net.UnknownHostException;

import voldemort.VoldemortApplicationException;


public class Host {

    private static InetAddress getLocal() {
        try {
            return InetAddress.getLocalHost();
        } catch(UnknownHostException ex) {
            throw new VoldemortApplicationException("Error retrieving local host", ex);
        }
    }

    public static String getFQDN() {
        return getLocal().getCanonicalHostName();
    }
}

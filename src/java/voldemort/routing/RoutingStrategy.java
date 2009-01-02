package voldemort.routing;

import java.util.List;
import java.util.Set;

import voldemort.cluster.Node;

/**
 * A routing strategy maps puts and gets to an ordered "preference list" of
 * servers. The preference list is the order under which operations will be
 * completed in the absence of failures.
 * 
 * @author jay
 * 
 */
public interface RoutingStrategy {

    /**
     * Get the node preference list for the given key. The preference list is a
     * list of nodes to perform an operation on.
     * 
     * @param key The key the operation is operating on
     * @return The preference list for the given key
     */
    public List<Node> routeRequest(byte[] key);

    /**
     * Get the collection of nodes that are candidates for routing.
     * 
     * @return The collection of nodes
     */
    public Set<Node> getNodes();

}

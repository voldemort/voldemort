package voldemort.store.slop;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;

import java.util.List;

/**
 * The equivalent of {@link RoutingStrategy} for hints
 */
public interface HintedHandoffStrategy {

    /**
     * Get a list an ordered "preference list" of nodes capable of receiving for a
     * given node in case of that node's failure.
     *
     * @param origin The original node which failed to receive the request
     * @return The list of nodes eligible to receive hints for original node
     */
    List<Node> routeHint(Node origin);
}

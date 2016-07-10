package voldemort.server;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;


public class NodeIdUtils {

    private static final Logger logger = Logger.getLogger(NodeIdUtils.class.getName());
    private static boolean isSingleLocalCluster(Cluster cluster, HostMatcher matcher) {
        if(cluster.getNumberOfNodes() == 1) {
            Node singleNode = cluster.getNodes().iterator().next();
            boolean isSingleLocalNode = matcher.isLocalAddress(singleNode);
            if(isSingleLocalNode) {
                logger.info(singleNode.briefToString() + " is a single local node cluster");
            }
            return isSingleLocalNode;
        }
        return false;
    }

    public static int findNodeId(Cluster cluster, HostMatcher matcher) {
        logger.info(" Using Matcher " + matcher.getDebugInfo());
        if(isSingleLocalCluster(cluster, matcher)) {
            int nodeId = cluster.getNodeIds().iterator().next();
            logger.info(" Cluster is a single local node cluster. Node Id " + nodeId);
            return nodeId;
        }

        List<Node> matches = Lists.newArrayList();
        for(Node node: cluster.getNodes()) {
            if(matcher.match(node)) {
                logger.info(node.briefToString() + " matched the current cluster ");
                matches.add(node);
            }
        }

        if(matches.isEmpty()) {
            throw new VoldemortApplicationException(" No nodes in the cluster matched the current node "
                                                + Arrays.toString(cluster.getNodes().toArray()));
        } else if ( matches.size() > 1) {
            String errorMessage = " More than one node matched "
                                  + Arrays.toString(matches.toArray());
            logger.error(errorMessage);
            throw new VoldemortApplicationException(errorMessage);
        } else {
            logger.info(" computed node Id match successfully " + matches.get(0).briefToString());
            return matches.get(0).getId();
        }
    }

    public static void validateNodeId(Cluster cluster, HostMatcher matcher, int nodeId) {
        logger.info(" Using Matcher " + matcher.getDebugInfo());
        // Make sure nodeId exists.
        cluster.getNodeById(nodeId);

        if(isSingleLocalCluster(cluster, matcher)) {
            return;
        }
        
        List<String> errors =  Lists.newArrayList();
        for(Node node: cluster.getNodes()) {
            if(nodeId == node.getId()) {
                if(!matcher.match(node)) {
                    errors.add(node.briefToString() + " selected as current node " + nodeId
                               + " failed to match");
                } 
            }else {
                if(matcher.match(node)) {
                    errors.add(node.briefToString() + " matched, though the expected node is "
                               + nodeId);
                }
            }
        }

        if(!errors.isEmpty()) {
            String errorMessage = " Number of Validation failures " + errors.size()
                                  + ". Details : " + Arrays.toString(errors.toArray());
            logger.error(errorMessage);
            if(errors.size() == 1) {
                throw new VoldemortApplicationException(errors.get(0));
            } else {
                throw new VoldemortApplicationException(errorMessage);
            }
        } else {
            logger.info("Node Id Validation succeeded. Node Id " + nodeId);
        }

    }
}

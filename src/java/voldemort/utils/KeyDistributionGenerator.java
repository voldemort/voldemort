package voldemort.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

public class KeyDistributionGenerator {

    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("cluster-xml", "[REQUIRED] cluster xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("stores-xml", "[REQUIRED] stores xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("num-keys", "Number of keys to query [Default:100000]")
              .withRequiredArg()
              .describedAs("number")
              .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "cluster-xml", "stores-xml");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        // compulsory params
        String clusterXml = (String) options.valueOf("cluster-xml");
        String storesXml = (String) options.valueOf("stores-xml");
        Integer numKeys = CmdUtils.valueOf(options, "num-keys", 100000);

        if(numKeys <= 0) {
            System.err.println("Number of keys should be greater than 0");
            System.exit(1);
        }

        Cluster cluster = new ClusterMapper().readCluster(new File(clusterXml));
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new File(storesXml));

        // Print distribution for every store

        for(StoreDefinition def: storeDef) {
            HashMap<Integer, Integer> storeDistribution = generateDistribution(cluster,
                                                                               def,
                                                                               numKeys);
            System.out.println("For Store " + def.getName());
            printDistribution(storeDistribution);
            System.out.println("=========================");
        }
    }

    static HashMap<Integer, Integer> generateDistribution(Cluster cluster,
                                                          StoreDefinition storeDef,
                                                          int numKeys) {
        RoutingStrategyFactory factory = new RoutingStrategyFactory();
        RoutingStrategy strategy = factory.updateRoutingStrategy(storeDef, cluster);

        HashMap<Integer, Long> requestRouting = Maps.newHashMap();
        Long total = new Long(0);
        for(int i = 0; i < numKeys; i++) {
            List<Node> nodes = strategy.routeRequest(("key" + i).getBytes());
            for(Node node: nodes) {
                Long count = requestRouting.get(node.getId());
                if(count == null) {
                    count = new Long(0);
                }
                count++;
                requestRouting.put(node.getId(), count);
                total++;
            }
        }
        HashMap<Integer, Integer> finalDistribution = Maps.newHashMap();
        for(int nodeId: requestRouting.keySet()) {
            finalDistribution.put(nodeId,
                                  new Integer((int) ((requestRouting.get(nodeId) * 100) / total)));
        }
        return finalDistribution;
    }

    static void printDistribution(HashMap<Integer, Integer> distribution) {
        for(int nodeId: distribution.keySet()) {
            System.out.println("Node " + nodeId + " - " + distribution.get(nodeId));
        }
    }

}

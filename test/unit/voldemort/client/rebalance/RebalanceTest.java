package voldemort.client.rebalance;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;

/**
 * Start VoldemortServer locally using ServerTestUtils and run rebalancing
 * tests.
 * 
 * @author bbansal
 * 
 */
public class RebalanceTest extends AbstractRebalanceTest {

    Map<Integer, VoldemortServer> serverMap = new HashMap<Integer, VoldemortServer>();

    @Override
    protected Cluster startServers(Cluster cluster,
                                   List<StoreDefinition> storeDefs,
                                   List<Integer> nodeToStart) throws IOException {
        for(int node: nodeToStart) {
            VoldemortServer server = ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(node,
                                                                                                             TestUtils.createTempDir()
                                                                                                                      .getAbsolutePath(),
                                                                                                             null,
                                                                                                             getStoresXmlFile()),
                                                                          cluster);
            serverMap.put(node, server);
        }

        return cluster;
    }

    @Override
    protected void stopServer(List<Integer> nodesToStop) throws IOException {
        for(int node: nodesToStop) {
            ServerTestUtils.stopVoldemortServer(serverMap.get(node));
        }
    }
}
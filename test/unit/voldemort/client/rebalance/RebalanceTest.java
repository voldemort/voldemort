package voldemort.client.rebalance;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.metadata.MetadataStore.VoldemortState;

/**
 * Start VoldemortServer locally using ServerTestUtils and run rebalancing
 * tests.
 * 
 * 
 */
@RunWith(Parameterized.class)
public class RebalanceTest extends AbstractRebalanceTest {

    Map<Integer, VoldemortServer> serverMap = new HashMap<Integer, VoldemortServer>();
    private final boolean useNio;
    private final boolean useDonorBased;

    public RebalanceTest(boolean useNio, boolean useDonorBased) {
        this.useNio = useNio;
        this.useDonorBased = useDonorBased;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, true }, { true, false }, { false, true },
                { false, false } });
    }

    @Override
    protected VoldemortState getCurrentState(int nodeId) {
        VoldemortServer server = serverMap.get(nodeId);
        if(server == null) {
            throw new VoldemortException("Node id " + nodeId + " does not exist");
        } else {
            return server.getMetadataStore().getServerState();
        }
    }

    @Override
    protected Cluster getCurrentCluster(int nodeId) {
        VoldemortServer server = serverMap.get(nodeId);
        if(server == null) {
            throw new VoldemortException("Node id " + nodeId + " does not exist");
        } else {
            return server.getMetadataStore().getCluster();
        }
    }

    @Override
    protected Cluster startServers(Cluster cluster,
                                   String storeXmlFile,
                                   List<Integer> nodeToStart,
                                   Map<String, String> configProps) throws IOException {
        for(int node: nodeToStart) {
            Properties properties = new Properties();
            if(null != configProps) {
                for(Entry<String, String> property: configProps.entrySet()) {
                    properties.put(property.getKey(), property.getValue());
                }
            }

            VoldemortConfig config = ServerTestUtils.createServerConfig(useNio,
                                                                        node,
                                                                        TestUtils.createTempDir()
                                                                                 .getAbsolutePath(),
                                                                        null,
                                                                        storeXmlFile,
                                                                        properties);

            VoldemortServer server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                          config,
                                                                          cluster);
            serverMap.put(node, server);
        }

        return cluster;
    }

    @Override
    protected void stopServer(List<Integer> nodesToStop) throws IOException {
        for(int node: nodesToStop) {
            try {
                ServerTestUtils.stopVoldemortServer(serverMap.get(node));
            } catch(VoldemortException e) {
                // ignore these at stop time
            }
        }
    }

    @Override
    protected boolean useDonorBased() {
        return this.useDonorBased;
    }
}

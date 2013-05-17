package voldemort.client.rebalance;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.rebalance.Rebalancer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * 
 * This test checks if we can interleave write to cluster and store metadata we
 * spwan a writer and a reader to see if we see an inconsistent state of the
 * system at any point
 * 
 */
public class RebalanceMetadataConsistencyTest {

    private MetadataStore metadataStore;
    private Cluster currentCluster;
    private Cluster targetCluster;

    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRW2 = "test2";

    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;
    private Rebalancer rebalancer;

    Cluster checkCluster;
    List<StoreDefinition> checkstores;

    /**
     * Convenient method to execute private methods from other classes.
     * 
     * @param test Instance of the class we want to test
     * @param methodName Name of the method we want to test
     * @param params Arguments we want to pass to the method
     * @return Object with the result of the executed method
     * @throws Exception
     */
    public static Object invokePrivateMethod(Object test, String methodName, Object params[])
            throws Exception {
        Object ret = null;

        final Method[] methods = test.getClass().getDeclaredMethods();
        for(int i = 0; i < methods.length; ++i) {
            if(methods[i].getName().equals(methodName)) {
                methods[i].setAccessible(true);
                ret = methods[i].invoke(test, params);
                break;
            }
        }

        return ret;
    }

    @Before
    public void setUp() {

        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0 }, { 1, 3 }, { 2 } });

        targetCluster = ServerTestUtils.getLocalCluster(3,
                                                        new int[][] { { 0 }, { 1 }, { 2 }, { 3 } });

        rwStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
        Store<String, String, String> innerStore = new InMemoryStorageEngine<String, String, String>("inner-store");
        innerStore.put(MetadataStore.CLUSTER_KEY,
                       new Versioned<String>(new ClusterMapper().writeCluster(currentCluster)),
                       null);
        innerStore.put(MetadataStore.STORES_KEY,
                       new Versioned<String>(new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(rwStoreDefWithReplication))),
                       null);

        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW2)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(2)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();

        metadataStore = new MetadataStore(innerStore, 0);
        rebalancer = new Rebalancer(null, metadataStore, null, null);

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testThreading() {

        for(int i = 0; i < 3000; i++) {

            Cluster cluster;
            StoreDefinition storeDef;
            if((i % 2) == 0) {
                cluster = currentCluster;
                storeDef = rwStoreDefWithReplication;

            } else {
                cluster = targetCluster;
                storeDef = rwStoreDefWithReplication2;

            }
            ThreadWriter tw = new ThreadWriter(cluster, storeDef);
            Thread writer = new Thread(tw);
            writer.start();
            ThreadReader tr = new ThreadReader();

            Thread reader = new Thread(tr);
            reader.start();
        }

    }

    class ThreadWriter implements Runnable {

        Cluster cluster;
        StoreDefinition storeDef;

        ThreadWriter(Cluster cluster, StoreDefinition storeDef) {

            this.cluster = cluster;
            this.storeDef = storeDef;
        }

        @Override
        public void run() {

            test();

        }

        public void test() {
            Object[] params = { MetadataStore.CLUSTER_KEY, this.cluster, MetadataStore.STORES_KEY,
                    Lists.newArrayList(this.storeDef) };
            try {
                invokePrivateMethod(rebalancer, "changeClusterAndStores", params);
            } catch(Exception e) {

                e.printStackTrace();
            }
        }

    }

    class ThreadReader implements Runnable {

        @Override
        public void run() {

            metadataStore.readLock.lock();
            checkCluster = metadataStore.getCluster();
            checkstores = metadataStore.getStoreDefList();
            metadataStore.readLock.unlock();

            if(checkCluster.equals(currentCluster)) {
                Assert.assertEquals(checkstores.get(0), rwStoreDefWithReplication);
            }
            if(checkCluster.equals(targetCluster)) {
                Assert.assertEquals(checkstores.get(0), rwStoreDefWithReplication2);
            }
        }
    }

}

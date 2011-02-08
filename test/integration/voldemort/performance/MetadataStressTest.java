package voldemort.performance;

import org.apache.log4j.Logger;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.xml.ClusterMapper;
import voldemort.xml.MappingException;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.StringReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class MetadataStressTest {

    private final static Logger logger = Logger.getLogger(MetadataStressTest.class);

    public static void main(String[] args) throws Exception {

        if(args.length < 3) {
            System.err.println("java voldemort.performance.MetadataStressTest url iterations threads selectors");
            System.exit(-1);
        }

        String url = args[0];
        final int count = Integer.parseInt(args[1]);
        int numThreads = Integer.parseInt(args[2]);
        int numSelectors = args.length > 3 ? Integer.parseInt(args[3]) : 8;
        int timeoutSecs = args.length > 4 ? Integer.parseInt(args[4]) : 10;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads,
                                                                new ThreadFactory() {

                                                                    public Thread newThread(Runnable r) {
                                                                        Thread thread = new Thread(r);
                                                                        thread.setName("stress-test");
                                                                        return thread;
                                                                    }
                                                                });
        try {
            final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(url)
                                                                                                    .setEnableLazy(false)
                                                                                                    .setConnectionTimeout(timeoutSecs, TimeUnit.SECONDS)
                                                                                                    .setSocketTimeout(timeoutSecs, TimeUnit.SECONDS)
                                                                                                    .setMaxThreads(numThreads)
                                                                                                    .setSelectors(numSelectors));
            for(int i = 0; i < numThreads; i++) {
                executor.submit(new Runnable() {

                    public void run() {
                        for(int j = 0; j < count; j++) {
                            try {
                                String clusterXml = factory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);
                                Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterXml));
                                String storesXml = factory.bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
                                List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml));
                                if(logger.isTraceEnabled())
                                    logger.trace("ok " + j);
                            } catch(MappingException me) {
                                logger.fatal(me, me);
                                System.exit(-1);
                            } catch(Exception e) {
                                logger.error(e, e);
                            }
                        }
                    }
                });
            }
        } finally {
          executor.shutdown();
        }
    }
}
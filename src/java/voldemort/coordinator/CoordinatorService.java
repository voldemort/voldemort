package voldemort.coordinator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.SystemStoreRepository;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.common.service.SchedulerService;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.SystemTime;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public class CoordinatorService {

    private static boolean noop = false;
    private static SocketStoreClientFactory storeClientFactory = null;
    private static String[] bootstrapURLs;
    private static AsyncMetadataVersionManager asyncMetadataManager = null;
    private static SchedulerService schedulerService = null;
    private static final Logger logger = Logger.getLogger(CoordinatorService.class);
    private static Map<String, FatClientWrapper> fatClientMap = null;
    private static long asyncMetadataCheckIntervalInMs = 5000;
    public final static Schema CLIENT_CONFIGS_AVRO_SCHEMA = Schema.parse("{ \"name\": \"clientConfigs\",  \"type\":\"array\","
                                                                         + "\"items\": { \"name\": \"clientConfig\", \"type\": \"map\", \"values\":\"string\" }}}");
    private static final String STORE_NAME_KEY = "store_name";

    private static void initializeFatClients() {
        StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
        String storesXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
        List<StoreDefinition> storeDefList = storeMapper.readStoreList(new StringReader(storesXml),
                                                                       false);
        Map<String, ClientConfig> fatClientConfigMap = readClientConfig("/home/csoman/Downloads/clientConfigs.avro",
                                                                        bootstrapURLs);
        // For now Simply create the map of store definition to
        // FatClientWrappers
        // TODO: After the fat client improvements is done, modify this to
        // - Fetch cluster.xml and stores.xml
        // - Pass these on to each FatClientWrapper
        // - Set up AsyncMetadataVersionManager
        fatClientMap = new HashMap<String, FatClientWrapper>();
        for(StoreDefinition storeDef: storeDefList) {
            String storeName = storeDef.getName();
            logger.info("Creating a Fat client wrapper for store: " + storeName);
            logger.info("Using config: " + fatClientConfigMap.get(storeName));
            fatClientMap.put(storeName, new FatClientWrapper(storeName,
                                                             bootstrapURLs,
                                                             fatClientConfigMap.get(storeName)));

        }

    }

    public static void main(String[] args) {

        if(args.length < 1) {
            System.err.println("Missing argument: <Bootstrap URL>");
            System.exit(-1);
        }

        if(args.length == 2) {
            if(args[1].equals("noop")) {
                noop = true;
            }
        }

        // Initialize the Voldemort Metadata
        bootstrapURLs = new String[1];
        bootstrapURLs[0] = args[0];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(bootstrapURLs);
        storeClientFactory = new SocketStoreClientFactory(clientConfig);
        initializeFatClients();

        // Setup the Async Metadata checker
        SystemStoreRepository sysRepository = new SystemStoreRepository();
        String clusterXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);

        sysRepository.createSystemStores(clientConfig,
                                         clusterXml,
                                         storeClientFactory.getFailureDetector());
        // Create a callback for re-bootstrapping the client
        Callable<Void> rebootstrapCallback = new Callable<Void>() {

            public Void call() throws Exception {
                initializeFatClients();
                return null;
            }

        };

        asyncMetadataManager = new AsyncMetadataVersionManager(sysRepository,
                                                               rebootstrapCallback,
                                                               null);

        schedulerService = new SchedulerService(1, SystemTime.INSTANCE, true);
        schedulerService.schedule(asyncMetadataManager.getClass().getName(),
                                  asyncMetadataManager,
                                  new Date(),
                                  asyncMetadataCheckIntervalInMs);

        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                                          Executors.newCachedThreadPool()));
        bootstrap.setOption("backlog", 1000);

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new CoordinatorPipelineFactory(fatClientMap, noop));

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(8080));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ClientConfig> readClientConfig(String configFilePath,
                                                              String[] bootstrapURLs) {
        String line;
        Map<String, ClientConfig> storeNameConfigMap = new HashMap<String, ClientConfig>();
        try {
            line = Joiner.on(" ")
                         .join(IOUtils.readLines(new FileReader(new File(configFilePath))))
                         .trim();

            JsonDecoder decoder = new JsonDecoder(CLIENT_CONFIGS_AVRO_SCHEMA, line);
            GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(CLIENT_CONFIGS_AVRO_SCHEMA);
            GenericData.Array<Map<Utf8, Utf8>> flowMaps = (GenericData.Array<Map<Utf8, Utf8>>) datumReader.read(null,
                                                                                                                decoder);

            // Flows to return back
            if(flowMaps != null && flowMaps.size() > 0) {
                for(Map<Utf8, Utf8> flowMap: flowMaps) {
                    Properties props = new Properties();
                    for(Utf8 key: flowMap.keySet()) {
                        props.put(key.toString(), flowMap.get(key).toString());
                    }

                    String storeName = flowMap.get(new Utf8(STORE_NAME_KEY)).toString();

                    storeName = props.getProperty(STORE_NAME_KEY);
                    if(storeName == null || storeName.length() == 0) {
                        throw new Exception("Illegal Store Name !!!");
                    }

                    ClientConfig config = new ClientConfig(props);
                    config.setBootstrapUrls(bootstrapURLs)
                          .setEnableCompressionLayer(false)
                          .setEnableSerializationLayer(false)
                          .enableDefaultClient(true)
                          .setEnableLazy(false);

                    storeNameConfigMap.put(storeName, config);

                }
            }
        } catch(FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return storeNameConfigMap;
    }
}

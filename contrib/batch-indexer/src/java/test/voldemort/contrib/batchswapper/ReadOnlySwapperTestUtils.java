package test.voldemort.contrib.batchswapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;

import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.store.StorageEngineType;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.JsonStoreBuilder;

public class ReadOnlySwapperTestUtils {

    /**
     * 
     * @param cluster
     * @param data
     * @param baseDir
     * @param TEST_SIZE
     * @return the directory where the index is created
     * @throws Exception
     */
    public static String createReadOnlyIndex(Cluster cluster,
                                             Map<String, String> data,
                                             String baseDir) throws Exception {
        // write data to file
        File dataFile = File.createTempFile("test", ".txt");
        dataFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile));
        for(Map.Entry<String, String> entry: data.entrySet())
            writer.write("\"" + entry.getKey() + "\"\t\"" + entry.getValue() + "\"\n");
        writer.close();
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        JsonReader jsonReader = new JsonReader(reader);

        SerializerDefinition serDef = new SerializerDefinition("json", "'string'");
        StoreDefinition storeDef = new StoreDefinition("test",
                                                       StorageEngineType.READONLY,
                                                       serDef,
                                                       serDef,
                                                       RoutingTier.CLIENT,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       1);
        RoutingStrategy router = new ConsistentRoutingStrategy(cluster.getNodes(), 1);

        // make a temp dir
        File dataDir = new File(baseDir + File.separatorChar + "read-only-temp-index-"
                                + new Integer((int) (Math.random() * 1000)));
        // build and open store
        JsonStoreBuilder storeBuilder = new JsonStoreBuilder(jsonReader,
                                                             cluster,
                                                             storeDef,
                                                             router,
                                                             dataDir,
                                                             100,
                                                             1);
        storeBuilder.build();

        return dataDir.getAbsolutePath();
    }
}
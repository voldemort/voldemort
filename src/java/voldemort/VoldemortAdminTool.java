/*
 * Copyright 2008-2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.StringSerializer;
import voldemort.serialization.avro.versioned.SchemaEvolutionValidator;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.CmdUtils;
import voldemort.utils.KeyDistributionGenerator;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Provides a command line interface to the
 * {@link voldemort.client.protocol.admin.AdminClient}
 */
public class VoldemortAdminTool {

    private static final String ALL_METADATA = "all";
    private static final String STORES_VERSION_KEY = "stores.xml";
    private static final String CLUSTER_VERSION_KEY = "cluster.xml";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("node", "node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("delete-partitions", "Delete partitions")
              .withRequiredArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("restore",
                       "Restore from replication [ Optional parallelism param - Default - 5 ]")
              .withOptionalArg()
              .describedAs("parallelism")
              .ofType(Integer.class);
        parser.accepts("ascii", "Fetch keys as ASCII");
        parser.accepts("fetch-keys", "Fetch keys")
              .withOptionalArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("fetch-entries", "Fetch full entries")
              .withOptionalArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("outdir", "Output directory")
              .withRequiredArg()
              .describedAs("output-directory")
              .ofType(String.class);
        parser.accepts("stores", "Store names")
              .withRequiredArg()
              .describedAs("store-names")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("store", "Store name for querying keys")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("add-stores", "Add stores in this stores.xml")
              .withRequiredArg()
              .describedAs("stores.xml containing just the new stores")
              .ofType(String.class);
        parser.accepts("delete-store", "Delete store")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("update-entries", "Insert or update entries")
              .withRequiredArg()
              .describedAs("input-directory")
              .ofType(String.class);
        parser.accepts("get-metadata",
                       "retreive metadata information " + MetadataStore.METADATA_KEYS)
              .withOptionalArg()
              .describedAs("metadata-key")
              .ofType(String.class);
        parser.accepts("check-metadata",
                       "retreive metadata information from all nodes and checks if they are consistent across [ "
                               + MetadataStore.CLUSTER_KEY + " | " + MetadataStore.STORES_KEY
                               + " | " + MetadataStore.SERVER_STATE_KEY + " ]")
              .withRequiredArg()
              .describedAs("metadata-key")
              .ofType(String.class);
        parser.accepts("ro-metadata",
                       "retrieve version information [current | max | storage-format]")
              .withRequiredArg()
              .describedAs("type")
              .ofType(String.class);
        parser.accepts("truncate", "truncate a store")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("set-metadata",
                       "Forceful setting of metadata [ " + MetadataStore.CLUSTER_KEY + " | "
                               + MetadataStore.STORES_KEY + " | " + MetadataStore.SERVER_STATE_KEY
                               + " | " + MetadataStore.REBALANCING_STEAL_INFO + " ]")
              .withRequiredArg()
              .describedAs("metadata-key")
              .ofType(String.class);
        parser.accepts("set-metadata-value",
                       "The value for the set-metadata [ " + MetadataStore.CLUSTER_KEY + " | "
                               + MetadataStore.STORES_KEY + ", "
                               + MetadataStore.REBALANCING_STEAL_INFO
                               + " ] - xml file location, [ " + MetadataStore.SERVER_STATE_KEY
                               + " ] - " + MetadataStore.VoldemortState.NORMAL_SERVER + ","
                               + MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER)
              .withRequiredArg()
              .describedAs("metadata-value")
              .ofType(String.class);
        parser.accepts("key-distribution", "Prints the current key distribution of the cluster");
        parser.accepts("clear-rebalancing-metadata", "Remove the metadata related to rebalancing");
        parser.accepts("async",
                       "a) Get a list of async job ids [get] b) Stop async job ids [stop] ")
              .withRequiredArg()
              .describedAs("op-type")
              .ofType(String.class);
        parser.accepts("async-id", "Comma separated list of async ids to stop")
              .withOptionalArg()
              .describedAs("job-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("repair-job", "Clean after rebalancing is done");
        parser.accepts("native-backup", "Perform a native backup")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("backup-dir")
              .withRequiredArg()
              .describedAs("backup-directory")
              .ofType(String.class);
        parser.accepts("backup-timeout")
              .withRequiredArg()
              .describedAs("minutes to wait for backup completion, default 30 mins")
              .ofType(Integer.class);
        parser.accepts("backup-verify",
                       "If provided, backup will also verify checksum (with extra overhead)");
        parser.accepts("backup-incremental",
                       "Perform an incremental backup for point-in-time recovery."
                               + " By default backup has latest consistent snapshot.");
        parser.accepts("zone", "zone id")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(Integer.class);
        parser.accepts("rollback", "rollback a store")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("version", "Push version of store to rollback to")
              .withRequiredArg()
              .describedAs("version")
              .ofType(Long.class);
        parser.accepts("verify-metadata-version",
                       "Verify the version of Metadata on all the cluster nodes");
        parser.accepts("synchronize-metadata-version",
                       "Synchronize the metadata versions across all the nodes.");
        parser.accepts("reserve-memory", "Memory in MB to reserve for the store")
              .withRequiredArg()
              .describedAs("size-in-mb")
              .ofType(Long.class);
        parser.accepts("query-keys", "Get values of keys on specific nodes")
              .withRequiredArg()
              .describedAs("query-keys")
              .withValuesSeparatedBy(',')
              .ofType(String.class);

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            printHelp(System.out, parser);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "url", "node");
        if(missing.size() > 0) {
            // Not the most elegant way to do this
            if(!(missing.equals(ImmutableSet.of("node"))
                 && (options.has("add-stores") || options.has("delete-store")
                     || options.has("ro-metadata") || options.has("set-metadata")
                     || options.has("get-metadata") || options.has("check-metadata") || options.has("key-distribution"))
                 || options.has("truncate") || options.has("clear-rebalancing-metadata")
                 || options.has("async") || options.has("native-backup") || options.has("rollback")
                 || options.has("verify-metadata-version") || options.has("reserve-memory"))) {
                System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
                printHelp(System.err, parser);
                System.exit(1);
            }
        }

        String url = (String) options.valueOf("url");
        Integer nodeId = CmdUtils.valueOf(options, "node", -1);
        int parallelism = CmdUtils.valueOf(options, "restore", 5);
        Integer zoneId = CmdUtils.valueOf(options, "zone", -1);

        int zone = zoneId == -1 ? 0 : zoneId;
        AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), zone);

        if(options.has("verify-metadata-version")) {
            checkMetadataVersion(adminClient);
            return;
        }

        String ops = "";
        if(options.has("delete-partitions")) {
            ops += "d";
        }
        if(options.has("fetch-keys")) {
            ops += "k";
        }
        if(options.has("fetch-entries")) {
            ops += "v";
        }
        if(options.has("restore")) {
            ops += "r";
        }
        if(options.has("add-stores")) {
            ops += "a";
        }
        if(options.has("update-entries")) {
            ops += "u";
        }
        if(options.has("delete-store")) {
            ops += "s";
        }
        if(options.has("get-metadata")) {
            ops += "g";
        }
        if(options.has("ro-metadata")) {
            ops += "e";
        }
        if(options.has("truncate")) {
            ops += "t";
        }
        if(options.has("set-metadata")) {
            ops += "m";
        }
        if(options.has("check-metadata")) {
            ops += "c";
        }
        if(options.has("key-distribution")) {
            ops += "y";
        }
        if(options.has("clear-rebalancing-metadata")) {
            ops += "i";
        }
        if(options.has("async")) {
            ops += "b";
        }
        if(options.has("repair-job")) {
            ops += "l";
        }
        if(options.has("native-backup")) {
            if(!options.has("backup-dir")) {
                Utils.croak("A backup directory must be specified with backup-dir option");
            }
            ops += "n";
        }
        if(options.has("rollback")) {
            if(!options.has("version")) {
                Utils.croak("A read-only push version must be specified with rollback option");
            }
            ops += "o";
        }
        if(options.has("synchronize-metadata-version")) {
            ops += "z";
        }
        if(options.has("reserve-memory")) {
            if(!options.has("stores")) {
                Utils.croak("Specify the list of stores to reserve memory");
            }
            ops += "f";
        }
        if(options.has("query-keys")) {
            ops += "q";
        }

        if(ops.length() < 1) {
            Utils.croak("At least one of (delete-partitions, restore, add-node, fetch-entries, "
                        + "fetch-keys, add-stores, delete-store, update-entries, get-metadata, ro-metadata, "
                        + "set-metadata, check-metadata, key-distribution, clear-rebalancing-metadata, async, "
                        + "repair-job, native-backup, rollback, reserve-memory, verify-metadata-version) must be specified");
        }

        List<String> storeNames = null;

        if(options.has("stores")) {
            List<String> temp = (List<String>) options.valuesOf("stores");
            storeNames = temp;
        }

        String outputDir = null;
        if(options.has("outdir")) {
            outputDir = (String) options.valueOf("outdir");
        }

        try {
            if(ops.contains("d")) {
                System.out.println("Starting delete-partitions");
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("delete-partitions");
                executeDeletePartitions(nodeId, adminClient, partitionIdList, storeNames);
                System.out.println("Finished delete-partitions");
            }
            if(ops.contains("r")) {
                if(nodeId == -1) {
                    System.err.println("Cannot run restore without node id");
                    System.exit(1);
                }
                System.out.println("Starting restore");
                adminClient.restoreDataFromReplications(nodeId, parallelism, zoneId);
                System.out.println("Finished restore");
            }
            if(ops.contains("k")) {
                boolean useAscii = options.has("ascii");
                System.out.println("Starting fetch keys");
                List<Integer> partitionIdList = null;
                if(options.hasArgument("fetch-keys"))
                    partitionIdList = (List<Integer>) options.valuesOf("fetch-keys");
                executeFetchKeys(nodeId,
                                 adminClient,
                                 partitionIdList,
                                 outputDir,
                                 storeNames,
                                 useAscii);
            }
            if(ops.contains("v")) {
                boolean useAscii = options.has("ascii");
                System.out.println("Starting fetch entries");
                List<Integer> partitionIdList = null;
                if(options.hasArgument("fetch-entries"))
                    partitionIdList = (List<Integer>) options.valuesOf("fetch-entries");
                executeFetchEntries(nodeId,
                                    adminClient,
                                    partitionIdList,
                                    outputDir,
                                    storeNames,
                                    useAscii);
            }
            if(ops.contains("a")) {
                String storesXml = (String) options.valueOf("add-stores");
                executeAddStores(adminClient, storesXml, nodeId);
            }
            if(ops.contains("u")) {
                String inputDir = (String) options.valueOf("update-entries");
                executeUpdateEntries(nodeId, adminClient, storeNames, inputDir);
            }
            if(ops.contains("s")) {
                String storeName = (String) options.valueOf("delete-store");
                executeDeleteStore(adminClient, storeName, nodeId);
            }
            if(ops.contains("g")) {
                String metadataKey = ALL_METADATA;
                if(options.hasArgument("get-metadata")) {
                    metadataKey = (String) options.valueOf("get-metadata");
                }
                executeGetMetadata(nodeId, adminClient, metadataKey, outputDir);
            }
            if(ops.contains("e")) {
                String type = (String) options.valueOf("ro-metadata");
                executeROMetadata(nodeId, adminClient, storeNames, type);
            }
            if(ops.contains("t")) {
                String storeName = (String) options.valueOf("truncate");
                executeTruncateStore(nodeId, adminClient, storeName);
            }
            if(ops.contains("c")) {
                String metadataKey = (String) options.valueOf("check-metadata");
                executeCheckMetadata(adminClient, metadataKey);
            }
            if(ops.contains("m")) {
                String metadataKey = (String) options.valueOf("set-metadata");
                if(!options.has("set-metadata-value")) {
                    throw new VoldemortException("Missing set-metadata-value");
                } else {
                    String metadataValue = (String) options.valueOf("set-metadata-value");
                    if(metadataKey.compareTo(MetadataStore.CLUSTER_KEY) == 0) {
                        if(!Utils.isReadableFile(metadataValue))
                            throw new VoldemortException("Cluster xml file path incorrect");
                        ClusterMapper mapper = new ClusterMapper();
                        Cluster newCluster = mapper.readCluster(new File(metadataValue));
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.CLUSTER_KEY,
                                           mapper.writeCluster(newCluster));
                    } else if(metadataKey.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                        VoldemortState newState = VoldemortState.valueOf(metadataValue);
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.SERVER_STATE_KEY,
                                           newState.toString());
                    } else if(metadataKey.compareTo(MetadataStore.STORES_KEY) == 0) {
                        if(!Utils.isReadableFile(metadataValue))
                            throw new VoldemortException("Stores definition xml file path incorrect");
                        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
                        List<StoreDefinition> storeDefs = mapper.readStoreList(new File(metadataValue));

                        String AVRO_GENERIC_VERSIONED_TYPE_NAME = "avro-generic-versioned";

                        for(StoreDefinition storeDef: storeDefs) {
                            SerializerDefinition keySerDef = storeDef.getKeySerializer();
                            SerializerDefinition valueSerDef = storeDef.getValueSerializer();

                            if(keySerDef.getName().equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)) {

                                SchemaEvolutionValidator.checkSchemaCompatibility(keySerDef);

                            }

                            if(valueSerDef.getName().equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)) {

                                SchemaEvolutionValidator.checkSchemaCompatibility(valueSerDef);

                            }
                        }
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.STORES_KEY,
                                           mapper.writeStoreList(storeDefs));
                        if(storeNames != null) {
                            System.out.println("Updating metadata version for the following stores: "
                                               + storeNames);
                            try {
                                for(String name: storeNames) {
                                    adminClient.updateMetadataversion(name);
                                }
                            } catch(Exception e) {
                                System.err.println("Error while updating metadata version for the specified store.");
                            }
                        }
                    } else if(metadataKey.compareTo(MetadataStore.REBALANCING_STEAL_INFO) == 0) {
                        if(!Utils.isReadableFile(metadataValue))
                            throw new VoldemortException("Rebalancing steal info file path incorrect");
                        String rebalancingStealInfoJsonString = FileUtils.readFileToString(new File(metadataValue));
                        RebalancerState state = RebalancerState.create(rebalancingStealInfoJsonString);
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.REBALANCING_STEAL_INFO,
                                           state.toJsonString());
                    } else {
                        throw new VoldemortException("Incorrect metadata key");
                    }
                }

            }
            if(ops.contains("y")) {
                executeKeyDistribution(adminClient);
            }
            if(ops.contains("i")) {
                executeClearRebalancing(nodeId, adminClient);
            }
            if(ops.contains("b")) {
                String asyncKey = (String) options.valueOf("async");
                List<Integer> asyncIds = null;
                if(options.hasArgument("async-id"))
                    asyncIds = (List<Integer>) options.valuesOf("async-id");
                executeAsync(nodeId, adminClient, asyncKey, asyncIds);
            }
            if(ops.contains("l")) {
                executeRepairJob(nodeId, adminClient);
            }
            if(ops.contains("n")) {
                String backupDir = (String) options.valueOf("backup-dir");
                String storeName = (String) options.valueOf("native-backup");
                int timeout = CmdUtils.valueOf(options, "backup-timeout", 30);
                adminClient.nativeBackup(nodeId,
                                         storeName,
                                         backupDir,
                                         timeout,
                                         options.has("backup-verify"),
                                         options.has("backup-incremental"));
            }
            if(ops.contains("o")) {
                String storeName = (String) options.valueOf("rollback");
                long pushVersion = (Long) options.valueOf("version");
                executeRollback(nodeId, storeName, pushVersion, adminClient);
            }
            if(ops.contains("z")) {
                synchronizeMetadataVersion(adminClient, nodeId);
            }
            if(ops.contains("f")) {
                long reserveMB = (Long) options.valueOf("reserve-memory");
                adminClient.reserveMemory(nodeId, storeNames, reserveMB);
            }
            if(ops.contains("q")) {
                List<String> keyList = (List<String>) options.valuesOf("query-keys");
                if(storeNames == null || storeNames.size() == 0) {
                    throw new VoldemortException("Must specify store name using --stores option");
                }
                executeQueryKeys(nodeId, adminClient, storeNames, keyList);
            }
        } catch(Exception e) {
            e.printStackTrace();
            Utils.croak(e.getMessage());
        }
    }

    private static String getMetadataVersionsForNode(AdminClient adminClient, int nodeId) {
        List<Integer> partitionIdList = Lists.newArrayList();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            partitionIdList.addAll(node.getPartitionIds());
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                                                partitionIdList,
                                                                                                null,
                                                                                                true);
        Serializer<String> serializer = new StringSerializer("UTF8");
        String keyObject = null;
        String valueObject = null;

        while(entriesIterator.hasNext()) {
            try {
                Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                byte[] valueBytes = kvPair.getSecond().getValue();
                keyObject = serializer.toObject(keyBytes);
                if(!keyObject.equals(MetadataVersionStoreUtils.VERSIONS_METADATA_KEY)) {
                    continue;
                }
                valueObject = serializer.toObject(valueBytes);
            } catch(Exception e) {
                System.err.println("Error while retrieving Metadata versions from node : " + nodeId
                                   + ". Exception = \n");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        return valueObject;
    }

    private static void checkMetadataVersion(AdminClient adminClient) {
        Map<Properties, Integer> versionsNodeMap = new HashMap<Properties, Integer>();

        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            String valueObject = getMetadataVersionsForNode(adminClient, node.getId());
            Properties props = new Properties();
            try {
                props.load(new ByteArrayInputStream(valueObject.getBytes()));
            } catch(IOException e) {
                System.err.println("Error while parsing Metadata versions for node : "
                                   + node.getId() + ". Exception = \n");
                e.printStackTrace();
                System.exit(-1);
            }

            versionsNodeMap.put(props, node.getId());
        }

        if(versionsNodeMap.keySet().size() > 1) {
            System.err.println("Mismatching versions detected !!!");
            for(Entry<Properties, Integer> entry: versionsNodeMap.entrySet()) {
                System.out.println("**************************** Node: " + entry.getValue()
                                   + " ****************************");
                System.out.println(entry.getKey());
            }
        } else {
            System.err.println("All the nodes have the same metadata versions .");
        }

    }

    private static void synchronizeMetadataVersion(AdminClient adminClient, int baseNodeId) {
        String valueObject = getMetadataVersionsForNode(adminClient, baseNodeId);
        Properties props = new Properties();
        try {
            props.load(new ByteArrayInputStream(valueObject.getBytes()));
            if(props.size() == 0) {
                System.err.println("The specified node does not have any versions metadata ! Exiting ...");
                System.exit(-1);
            }
            adminClient.setMetadataversion(props);
            System.out.println("Metadata versions synchronized successfully.");
        } catch(IOException e) {
            System.err.println("Error while retrieving Metadata versions from node : " + baseNodeId
                               + ". Exception = \n");
            e.printStackTrace();
            System.exit(-1);
        }

    }

    private static void executeRollback(Integer nodeId,
                                        String storeName,
                                        long pushVersion,
                                        AdminClient adminClient) {
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                adminClient.rollbackStore(node.getId(), storeName, pushVersion);
            }
        } else {
            adminClient.rollbackStore(nodeId, storeName, pushVersion);
        }
    }

    private static void executeRepairJob(Integer nodeId, AdminClient adminClient) {
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                adminClient.repairJob(node.getId());
            }
        } else {
            adminClient.repairJob(nodeId);
        }
    }

    public static void printHelp(PrintStream stream, OptionParser parser) throws IOException {
        stream.println("Commands supported");
        stream.println("------------------");
        stream.println("CHANGE METADATA");
        stream.println("\t1) Get all metadata from all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --get-metadata --url [url]");
        stream.println("\t2) Get metadata from all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --get-metadata "
                       + MetadataStore.METADATA_KEYS + " --url [url]");
        stream.println("\t3) Get metadata from a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --get-metadata "
                       + MetadataStore.METADATA_KEYS + " --url [url] --node [node-id]");
        stream.println("\t4) Get metadata from a particular node and store to a directory");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --get-metadata "
                       + MetadataStore.METADATA_KEYS
                       + " --url [url] --node [node-id] --outdir [directory]");
        stream.println("\t5) Set metadata on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --set-metadata ["
                       + MetadataStore.CLUSTER_KEY + ", " + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.STORES_KEY + ", " + MetadataStore.REBALANCING_STEAL_INFO
                       + "] --set-metadata-value [metadata-value] --url [url]");
        stream.println("\t6) Set metadata for a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --set-metadata ["
                       + MetadataStore.CLUSTER_KEY + ", " + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.STORES_KEY + ", " + MetadataStore.REBALANCING_STEAL_INFO
                       + "] --set-metadata-value [metadata-value] --url [url] --node [node-id]");
        stream.println("\t7) Check if metadata is same on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --check-metadata ["
                       + MetadataStore.CLUSTER_KEY + ", " + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.STORES_KEY + "] --url [url]");
        stream.println("\t8) Clear rebalancing metadata [" + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.REBALANCING_STEAL_INFO + "] on all node ");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --clear-rebalancing-metadata --url [url]");
        stream.println("\t9) Clear rebalancing metadata [" + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.REBALANCING_STEAL_INFO + "] on a particular node ");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --clear-rebalancing-metadata --url [url] --node [node-id]");
        stream.println();
        stream.println("ADD / DELETE STORES");
        stream.println("\t1) Add store(s) on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --add-stores [xml file with store(s) to add] --url [url]");
        stream.println("\t2) Add store(s) on a single node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --add-stores [xml file with store(s) to add] --url [url] --node [node-id]");
        stream.println("\t3) Delete store on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --delete-store [store-name] --url [url]");
        stream.println("\t4) Delete store on a single node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --delete-store [store-name] --url [url] --node [node-id]");
        stream.println("\t5) Delete the contents of the store on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --truncate [store-name] --url [url]");
        stream.println("\t6) Delete the contents of the store on a single node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --truncate [store-name] --url [url] --node [node-id]");
        stream.println("\t7) Delete the contents of some partitions on a single node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --delete-partitions [comma-separated list of partitions] --url [url] --node [node-id]");
        stream.println("\t8) Delete the contents of some partitions ( of some stores ) on a single node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --delete-partitions [comma-separated list of partitions] --url [url] --node [node-id] --stores [comma-separated list of store names]");
        stream.println();
        stream.println("STREAM DATA");
        stream.println("\t1) Fetch keys from a set of partitions [ all stores ] on a node ( binary dump )");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-keys [comma-separated list of partitions with no space] --url [url] --node [node-id]");
        stream.println("\t2) Fetch keys from a set of partitions [ all stores ] on a node ( ascii enabled )");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-keys [comma-separated list of partitions with no space] --url [url] --node [node-id] --ascii");
        stream.println("\t3) Fetch entries from a set of partitions [ all stores ] on a node ( binary dump )");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-entries [comma-separated list of partitions with no space] --url [url] --node [node-id]");
        stream.println("\t4) Fetch entries from a set of partitions [ all stores ] on a node ( ascii enabled )");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-entries [comma-separated list of partitions with no space] --url [url] --node [node-id] --ascii");
        stream.println("\t5) Fetch entries from a set of partitions [ all stores ] on a node ( ascii enabled ) and output to a folder");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-entries [comma-separated list of partitions with no space] --url [url] --node [node-id] --ascii --outdir [directory]");
        stream.println("\t6) Fetch entries from a set of partitions and some stores on a node ( ascii enabled )");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-entries [comma-separated list of partitions with no space] --url [url] --node [node-id] --ascii --stores [comma-separated list of store names] ");
        stream.println("\t7) Fetch all keys on a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-keys --url [url] --node [node-id]");
        stream.println("\t8) Fetch all entries on a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-entries --url [url] --node [node-id]");
        stream.println("\t9) Update entries for a set of stores using the output from a binary dump fetch entries");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --update-entries [folder path from output of --fetch-entries --outdir] --url [url] --node [node-id] --stores [comma-separated list of store names]");
        stream.println("\t10) Query stores for a set of keys on a specific node.");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --query-keys [comma-separated list of keys] --url [url] --node [node-id] --stores [comma-separated list of store names]");
        stream.println();
        stream.println("READ-ONLY OPERATIONS");
        stream.println("\t1) Retrieve metadata information of read-only data for a particular node and all stores");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --ro-metadata [current | max | storage-format] --url [url] --node [node-id]");
        stream.println("\t2) Retrieve metadata information of read-only data for all nodes and a set of store");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --ro-metadata [current | max | storage-format] --url [url] --stores [comma-separated list of store names]");
        stream.println();
        stream.println("ASYNC JOBS");
        stream.println("\t1) Get a list of async jobs on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --async get --url [url]");
        stream.println("\t2) Get a list of async jobs on a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --async get --url [url] --node [node-id]");
        stream.println("\t3) Stop a list of async jobs on a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --async stop --async-id [comma-separated list of async job id] --url [url] --node [node-id]");
        stream.println();
        stream.println("OTHERS");
        stream.println("\t1) Restore a particular node completely from its replicas");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --restore --url [url] --node [node-id]");
        stream.println("\t2) Restore a particular node completely from its replicas ( with increased parallelism - 10 ) ");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --restore 10 --url [url] --node [node-id]");
        stream.println("\t3) Generates the key distribution on a per node basis [ both store wise and overall ]");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --key-distribution --url [url]");
        stream.println("\t4) Clean a node after rebalancing is done");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --repair-job --url [url] --node [node-id]");
        stream.println("\t5) Backup bdb data natively");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --native-backup [store] --backup-dir [outdir] "
                       + "--backup-timeout [mins] [--backup-verify] [--backup-incremental] --url [url] --node [node-id]");
        stream.println("\t6) Rollback a read-only store to the specified push version");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --rollback [store-name] --url [url] --node [node-id] --version [version-num] ");

        parser.printHelpOn(stream);
    }

    private static void executeAsync(Integer nodeId,
                                     AdminClient adminClient,
                                     String asyncKey,
                                     List<Integer> asyncIdsToStop) {

        if(asyncKey.compareTo("get") == 0) {
            List<Integer> nodeIds = Lists.newArrayList();
            if(nodeId < 0) {
                for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                    nodeIds.add(node.getId());
                }
            } else {
                nodeIds.add(nodeId);
            }

            // Print the job information
            for(int currentNodeId: nodeIds) {
                System.out.println("Retrieving async jobs from node " + currentNodeId);
                List<Integer> asyncIds = adminClient.getAsyncRequestList(currentNodeId);
                System.out.println("Async Job Ids on node " + currentNodeId + " : " + asyncIds);
                for(int asyncId: asyncIds) {
                    System.out.println("Async Job Id " + asyncId + " ] "
                                       + adminClient.getAsyncRequestStatus(currentNodeId, asyncId));
                    System.out.println();
                }
            }
        } else if(asyncKey.compareTo("stop") == 0) {
            if(nodeId < 0) {
                throw new VoldemortException("Cannot stop job ids without node id");
            }

            if(asyncIdsToStop == null || asyncIdsToStop.size() == 0) {
                throw new VoldemortException("Async ids cannot be null / zero");
            }

            for(int asyncId: asyncIdsToStop) {
                System.out.println("Stopping async id " + asyncId);
                adminClient.stopAsyncRequest(nodeId, asyncId);
                System.out.println("Stopped async id " + asyncId);
            }
        } else {
            throw new VoldemortException("Unsupported async operation type " + asyncKey);
        }

    }

    private static void executeClearRebalancing(int nodeId, AdminClient adminClient) {
        System.out.println("Setting " + MetadataStore.SERVER_STATE_KEY + " to "
                           + MetadataStore.VoldemortState.NORMAL_SERVER);
        executeSetMetadata(nodeId,
                           adminClient,
                           MetadataStore.SERVER_STATE_KEY,
                           MetadataStore.VoldemortState.NORMAL_SERVER.toString());
        RebalancerState state = RebalancerState.create("[]");
        System.out.println("Cleaning up " + MetadataStore.REBALANCING_STEAL_INFO + " to "
                           + state.toJsonString());
        executeSetMetadata(nodeId,
                           adminClient,
                           MetadataStore.REBALANCING_STEAL_INFO,
                           state.toJsonString());

    }

    private static void executeKeyDistribution(AdminClient adminClient) {
        List<ByteArray> keys = KeyDistributionGenerator.generateKeys(KeyDistributionGenerator.DEFAULT_NUM_KEYS);
        System.out.println(KeyDistributionGenerator.printStoreWiseDistribution(adminClient.getAdminClientCluster(),
                                                                               adminClient.getRemoteStoreDefList(0)
                                                                                          .getValue(),
                                                                               keys));
        System.out.println(KeyDistributionGenerator.printOverallDistribution(adminClient.getAdminClientCluster(),
                                                                             adminClient.getRemoteStoreDefList(0)
                                                                                        .getValue(),
                                                                             keys));
    }

    private static void executeCheckMetadata(AdminClient adminClient, String metadataKey) {

        Set<Object> metadataValues = Sets.newHashSet();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            System.out.println(node.getHost() + ":" + node.getId());
            Versioned<String> versioned = adminClient.getRemoteMetadata(node.getId(), metadataKey);
            if(versioned == null || versioned.getValue() == null) {
                throw new VoldemortException("Value returned from node " + node.getId()
                                             + " was null");
            } else {

                if(metadataKey.compareTo(MetadataStore.CLUSTER_KEY) == 0) {
                    metadataValues.add(new ClusterMapper().readCluster(new StringReader(versioned.getValue())));
                } else if(metadataKey.compareTo(MetadataStore.STORES_KEY) == 0) {
                    metadataValues.add(new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue())));
                } else if(metadataKey.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                    metadataValues.add(VoldemortState.valueOf(versioned.getValue()));
                } else {
                    throw new VoldemortException("Incorrect metadata key");
                }

            }
        }

        if(metadataValues.size() == 1) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }

    public static void executeSetMetadata(Integer nodeId,
                                          AdminClient adminClient,
                                          String key,
                                          Object value) {

        List<Integer> nodeIds = Lists.newArrayList();
        VectorClock updatedVersion = null;
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
                if(updatedVersion == null) {
                    updatedVersion = (VectorClock) adminClient.getRemoteMetadata(node.getId(), key)
                                                              .getVersion();
                } else {
                    updatedVersion = updatedVersion.merge((VectorClock) adminClient.getRemoteMetadata(node.getId(),
                                                                                                      key)
                                                                                   .getVersion());
                }
            }

            // Bump up version on node 0
            updatedVersion = updatedVersion.incremented(0, System.currentTimeMillis());
        } else {
            Versioned<String> currentValue = adminClient.getRemoteMetadata(nodeId, key);
            updatedVersion = ((VectorClock) currentValue.getVersion()).incremented(nodeId,
                                                                                   System.currentTimeMillis());
            nodeIds.add(nodeId);
        }
        adminClient.updateRemoteMetadata(nodeIds,
                                         key,
                                         Versioned.value(value.toString(), updatedVersion));
    }

    private static void executeROMetadata(Integer nodeId,
                                          AdminClient adminClient,
                                          List<String> storeNames,
                                          String type) {
        Map<String, Long> storeToValue = Maps.newHashMap();

        if(storeNames == null) {
            // Retrieve list of read-only stores
            storeNames = Lists.newArrayList();
            for(StoreDefinition storeDef: adminClient.getRemoteStoreDefList(nodeId > 0 ? nodeId : 0)
                                                     .getValue()) {
                if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
                    storeNames.add(storeDef.getName());
                }
            }
        }

        List<Integer> nodeIds = Lists.newArrayList();
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
            }
        } else {
            nodeIds.add(nodeId);
        }

        for(int currentNodeId: nodeIds) {
            System.out.println(adminClient.getAdminClientCluster()
                                          .getNodeById(currentNodeId)
                                          .getHost()
                               + ":"
                               + adminClient.getAdminClientCluster()
                                            .getNodeById(currentNodeId)
                                            .getId());
            if(type.compareTo("max") == 0) {
                storeToValue = adminClient.getROMaxVersion(currentNodeId, storeNames);
            } else if(type.compareTo("current") == 0) {
                storeToValue = adminClient.getROCurrentVersion(currentNodeId, storeNames);
            } else if(type.compareTo("storage-format") == 0) {
                Map<String, String> storeToStorageFormat = adminClient.getROStorageFormat(currentNodeId,
                                                                                          storeNames);
                for(String storeName: storeToStorageFormat.keySet()) {
                    System.out.println(storeName + ":" + storeToStorageFormat.get(storeName));
                }
                continue;
            } else {
                System.err.println("Unsupported operation, only max, current or storage-format allowed");
                return;
            }

            for(String storeName: storeToValue.keySet()) {
                System.out.println(storeName + ":" + storeToValue.get(storeName));
            }
        }

    }

    private static void executeGetMetadata(Integer nodeId,
                                           AdminClient adminClient,
                                           String metadataKey,
                                           String outputDir) throws IOException {
        File directory = null;
        if(outputDir != null) {
            directory = new File(outputDir);
            if(!(directory.exists() || directory.mkdir())) {
                Utils.croak("Can't find or create directory " + outputDir);
            }
        }

        List<Integer> nodeIds = Lists.newArrayList();
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
            }
        } else {
            nodeIds.add(nodeId);
        }

        List<String> metadataKeys = Lists.newArrayList();
        if(metadataKey.compareTo(ALL_METADATA) == 0) {
            for(Object key: MetadataStore.METADATA_KEYS) {
                metadataKeys.add((String) key);
            }
        } else {
            metadataKeys.add(metadataKey);
        }
        for(Integer currentNodeId: nodeIds) {
            System.out.println(adminClient.getAdminClientCluster()
                                          .getNodeById(currentNodeId)
                                          .getHost()
                               + ":"
                               + adminClient.getAdminClientCluster()
                                            .getNodeById(currentNodeId)
                                            .getId());
            for(String key: metadataKeys) {
                System.out.println("Key - " + key);
                Versioned<String> versioned = null;
                try {
                    versioned = adminClient.getRemoteMetadata(currentNodeId, key);
                } catch(Exception e) {
                    System.out.println("Error in retrieving " + e.getMessage());
                    System.out.println();
                    continue;
                }
                if(versioned == null) {
                    if(directory == null) {
                        System.out.println("null");
                        System.out.println();
                    } else {
                        FileUtils.writeStringToFile(new File(directory, key + "_" + currentNodeId),
                                                    "");
                    }
                } else {
                    if(directory == null) {
                        System.out.println(versioned.getVersion());
                        System.out.print(": ");
                        System.out.println(versioned.getValue());
                        System.out.println();
                    } else {
                        FileUtils.writeStringToFile(new File(directory, key + "_" + currentNodeId),
                                                    versioned.getValue());
                    }
                }
            }
        }
    }

    private static void executeDeleteStore(AdminClient adminClient, String storeName, int nodeId) {
        System.out.println("Deleting " + storeName);
        if(nodeId == -1) {
            adminClient.deleteStore(storeName);
        } else {
            adminClient.deleteStore(storeName, nodeId);
        }

    }

    private static void executeTruncateStore(int nodeId, AdminClient adminClient, String storeName) {
        List<Integer> nodeIds = Lists.newArrayList();
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
            }
        } else {
            nodeIds.add(nodeId);
        }

        for(Integer currentNodeId: nodeIds) {
            System.out.println("Truncating " + storeName + " on node " + currentNodeId);
            adminClient.truncate(currentNodeId, storeName);
        }
    }

    private static void executeAddStores(AdminClient adminClient, String storesXml, int nodeId)
            throws IOException {
        List<StoreDefinition> storeDefinitionList = new StoreDefinitionsMapper().readStoreList(new File(storesXml));
        for(StoreDefinition storeDef: storeDefinitionList) {
            System.out.println("Adding " + storeDef.getName());
            if(-1 != nodeId)
                adminClient.addStore(storeDef, nodeId);
            else
                adminClient.addStore(storeDef);
        }
    }

    private static void executeFetchEntries(Integer nodeId,
                                            AdminClient adminClient,
                                            List<Integer> partitionIdList,
                                            String outputDir,
                                            List<String> storeNames,
                                            boolean useAscii) throws IOException {

        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                               .getValue();
        HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        File directory = null;
        if(outputDir != null) {
            directory = new File(outputDir);
            if(!(directory.exists() || directory.mkdir())) {
                Utils.croak("Can't find or create directory " + outputDir);
            }
        }
        List<String> stores = storeNames;
        if(stores == null) {
            // when no stores specified, all user defined store will be fetched,
            // but not system stores.
            stores = Lists.newArrayList();
            stores.addAll(storeDefinitionMap.keySet());
        } else {
            // add system stores to the map so they can be fetched when
            // specified explicitly
            storeDefinitionMap.putAll(getSystemStoreDefs());
        }

        // Pick up all the partitions
        if(partitionIdList == null) {
            partitionIdList = Lists.newArrayList();
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                partitionIdList.addAll(node.getPartitionIds());
            }
        }

        StoreDefinition storeDefinition = null;
        for(String store: stores) {
            storeDefinition = storeDefinitionMap.get(store);

            if(null == storeDefinition) {

                System.out.println("No store found under the name \'" + store + "\'");
                continue;
            } else {
                System.out.println("Fetching entries in partitions "
                                   + Joiner.on(", ").join(partitionIdList) + " of " + store);
            }

            final Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                          store,
                                                                                                          partitionIdList,
                                                                                                          null,
                                                                                                          false);
            File outputFile = null;
            if(directory != null) {
                outputFile = new File(directory, store + ".entries");
            }

            if(useAscii) {
                // k-v serializer
                SerializerDefinition keySerializerDef = storeDefinition.getKeySerializer();
                SerializerDefinition valueSerializerDef = storeDefinition.getValueSerializer();
                SerializerFactory serializerFactory = new DefaultSerializerFactory();
                @SuppressWarnings("unchecked")
                final Serializer<Object> keySerializer = (Serializer<Object>) serializerFactory.getSerializer(keySerializerDef);
                @SuppressWarnings("unchecked")
                final Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(valueSerializerDef);

                // compression strategy
                final CompressionStrategy keyCompressionStrategy;
                final CompressionStrategy valueCompressionStrategy;
                if(keySerializerDef != null && keySerializerDef.hasCompression()) {
                    keyCompressionStrategy = new CompressionStrategyFactory().get(keySerializerDef.getCompression());
                } else {
                    keyCompressionStrategy = null;
                }
                if(valueSerializerDef != null && valueSerializerDef.hasCompression()) {
                    valueCompressionStrategy = new CompressionStrategyFactory().get(valueSerializerDef.getCompression());
                } else {
                    valueCompressionStrategy = null;
                }

                writeAscii(outputFile, new Writable() {

                    @Override
                    public void writeTo(BufferedWriter out) throws IOException {
                        final StringWriter stringWriter = new StringWriter();
                        final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

                        while(entriesIterator.hasNext()) {
                            Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                            byte[] keyBytes = kvPair.getFirst().get();
                            byte[] valueBytes = kvPair.getSecond().getValue();
                            VectorClock version = (VectorClock) kvPair.getSecond().getVersion();

                            Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                                      : keyCompressionStrategy.inflate(keyBytes));
                            Object valueObject = valueSerializer.toObject((null == valueCompressionStrategy) ? valueBytes
                                                                                                            : valueCompressionStrategy.inflate(valueBytes));
                            generator.writeObject(keyObject);
                            stringWriter.write(' ');
                            stringWriter.write(version.toString());
                            generator.writeObject(valueObject);

                            StringBuffer buf = stringWriter.getBuffer();
                            if(buf.charAt(0) == ' ') {
                                buf.setCharAt(0, '\n');
                            }
                            out.write(buf.toString());
                            buf.setLength(0);
                        }
                        out.write('\n');
                    }
                });
            } else {
                writeBinary(outputFile, new Printable() {

                    @Override
                    public void printTo(DataOutputStream out) throws IOException {
                        while(entriesIterator.hasNext()) {
                            Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                            byte[] keyBytes = kvPair.getFirst().get();
                            byte[] versionBytes = ((VectorClock) kvPair.getSecond().getVersion()).toBytes();
                            byte[] valueBytes = kvPair.getSecond().getValue();
                            out.writeInt(keyBytes.length);
                            out.write(keyBytes);
                            out.writeInt(versionBytes.length);
                            out.write(versionBytes);
                            out.writeInt(valueBytes.length);
                            out.write(valueBytes);
                        }
                    }
                });
            }

            if(outputFile != null)
                System.out.println("Fetched keys from " + store + " to " + outputFile);
        }
    }

    private static Map<String, StoreDefinition> getSystemStoreDefs() {
        Map<String, StoreDefinition> sysStoreDefMap = Maps.newHashMap();
        List<StoreDefinition> storesDefs = SystemStoreConstants.getAllSystemStoreDefs();
        for(StoreDefinition def: storesDefs) {
            sysStoreDefMap.put(def.getName(), def);
        }
        return sysStoreDefMap;
    }

    private static void executeUpdateEntries(Integer nodeId,
                                             AdminClient adminClient,
                                             List<String> storeNames,
                                             String inputDirPath) throws IOException {
        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                               .getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        File inputDir = new File(inputDirPath);
        if(!inputDir.exists()) {
            throw new FileNotFoundException("input directory " + inputDirPath + " doesn't exist");
        }

        if(storeNames == null) {
            storeNames = Lists.newArrayList();
            for(File storeFile: inputDir.listFiles()) {
                String fileName = storeFile.getName();
                if(fileName.endsWith(".entries")) {
                    int extPosition = fileName.lastIndexOf(".entries");
                    storeNames.add(fileName.substring(0, extPosition));
                }
            }
        }

        for(String storeName: storeNames) {
            Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = readEntriesBinary(inputDir,
                                                                                      storeName);
            adminClient.updateEntries(nodeId, storeName, iterator, null);
        }

    }

    private static Iterator<Pair<ByteArray, Versioned<byte[]>>> readEntriesBinary(File inputDir,
                                                                                  String storeName)
            throws IOException {
        File inputFile = new File(inputDir, storeName + ".entries");
        if(!inputFile.exists()) {
            throw new FileNotFoundException("File " + inputFile.getAbsolutePath()
                                            + " does not exist!");
        }
        final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)));

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                try {
                    int length = dis.readInt();
                    byte[] keyBytes = new byte[length];
                    ByteUtils.read(dis, keyBytes);
                    length = dis.readInt();
                    byte[] versionBytes = new byte[length];
                    ByteUtils.read(dis, versionBytes);
                    length = dis.readInt();
                    byte[] valueBytes = new byte[length];
                    ByteUtils.read(dis, valueBytes);

                    ByteArray key = new ByteArray(keyBytes);
                    VectorClock version = new VectorClock(versionBytes);
                    Versioned<byte[]> value = new Versioned<byte[]>(valueBytes, version);

                    return new Pair<ByteArray, Versioned<byte[]>>(key, value);
                } catch(EOFException e) {
                    try {
                        dis.close();
                    } catch(IOException ie) {
                        ie.printStackTrace();
                    }
                    return endOfData();
                } catch(IOException e) {
                    try {
                        dis.close();
                    } catch(IOException ie) {
                        ie.printStackTrace();
                    }
                    throw new VoldemortException("Error reading from input file ", e);
                }
            }
        };
    }

    private static void executeFetchKeys(Integer nodeId,
                                         AdminClient adminClient,
                                         List<Integer> partitionIdList,
                                         String outputDir,
                                         List<String> storeNames,
                                         boolean useAscii) throws IOException {
        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                               .getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        File directory = null;
        if(outputDir != null) {
            directory = new File(outputDir);
            if(!(directory.exists() || directory.mkdir())) {
                Utils.croak("Can't find or create directory " + outputDir);
            }
        }

        List<String> stores = storeNames;
        if(stores == null) {
            stores = Lists.newArrayList();
            stores.addAll(storeDefinitionMap.keySet());
        } else {
            // add system stores to the map so they can be fetched when
            // specified explicitly
            storeDefinitionMap.putAll(getSystemStoreDefs());
        }

        // Pick up all the partitions
        if(partitionIdList == null) {
            partitionIdList = Lists.newArrayList();
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                partitionIdList.addAll(node.getPartitionIds());
            }
        }

        StoreDefinition storeDefinition = null;
        for(String store: stores) {
            storeDefinition = storeDefinitionMap.get(store);

            if(null == storeDefinition) {
                System.out.println("No store found under the name \'" + store + "\'");
                continue;
            } else {
                System.out.println("Fetching keys in partitions "
                                   + Joiner.on(", ").join(partitionIdList) + " of " + store);
            }

            final Iterator<ByteArray> keyIterator = adminClient.fetchKeys(nodeId,
                                                                          store,
                                                                          partitionIdList,
                                                                          null,
                                                                          false);
            File outputFile = null;
            if(directory != null) {
                outputFile = new File(directory, store + ".keys");
            }

            if(useAscii) {
                final SerializerDefinition serializerDef = storeDefinition.getKeySerializer();
                final SerializerFactory serializerFactory = new DefaultSerializerFactory();
                @SuppressWarnings("unchecked")
                final Serializer<Object> serializer = (Serializer<Object>) serializerFactory.getSerializer(serializerDef);

                final CompressionStrategy keysCompressionStrategy;
                if(serializerDef != null && serializerDef.hasCompression()) {
                    keysCompressionStrategy = new CompressionStrategyFactory().get(serializerDef.getCompression());
                } else {
                    keysCompressionStrategy = null;
                }

                writeAscii(outputFile, new Writable() {

                    @Override
                    public void writeTo(BufferedWriter out) throws IOException {
                        final StringWriter stringWriter = new StringWriter();
                        final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

                        while(keyIterator.hasNext()) {
                            // Ugly hack to be able to separate text by newlines
                            // vs. spaces
                            byte[] keyBytes = keyIterator.next().get();
                            Object keyObject = serializer.toObject((null == keysCompressionStrategy) ? keyBytes
                                                                                                    : keysCompressionStrategy.inflate(keyBytes));
                            generator.writeObject(keyObject);
                            StringBuffer buf = stringWriter.getBuffer();
                            if(buf.charAt(0) == ' ') {
                                buf.setCharAt(0, '\n');
                            }
                            out.write(buf.toString());
                            buf.setLength(0);
                        }
                        out.write('\n');
                    }
                });
            } else {
                writeBinary(outputFile, new Printable() {

                    @Override
                    public void printTo(DataOutputStream out) throws IOException {
                        while(keyIterator.hasNext()) {
                            byte[] keyBytes = keyIterator.next().get();
                            out.writeInt(keyBytes.length);
                            out.write(keyBytes);
                        }
                    }
                });
            }

            if(outputFile != null)
                System.out.println("Fetched keys from " + store + " to " + outputFile);
        }
    }

    private abstract static class Printable {

        public abstract void printTo(DataOutputStream out) throws IOException;
    }

    private abstract static class Writable {

        public abstract void writeTo(BufferedWriter out) throws IOException;
    }

    private static void writeBinary(File outputFile, Printable printable) throws IOException {
        OutputStream outputStream = null;
        if(outputFile == null) {
            outputStream = new FilterOutputStream(System.out) {

                @Override
                public void close() throws IOException {
                    flush();
                }
            };
        } else {
            outputStream = new FileOutputStream(outputFile);
        }
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));
        try {
            printable.printTo(dataOutputStream);
        } finally {
            dataOutputStream.close();
        }
    }

    private static void writeAscii(File outputFile, Writable writable) throws IOException {
        Writer writer = null;
        if(outputFile == null) {
            writer = new OutputStreamWriter(new FilterOutputStream(System.out) {

                @Override
                public void close() throws IOException {
                    flush();
                }
            });
        } else {
            writer = new FileWriter(outputFile);
        }
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        try {
            writable.writeTo(bufferedWriter);
        } finally {
            bufferedWriter.close();
        }
    }

    private static void executeDeletePartitions(Integer nodeId,
                                                AdminClient adminClient,
                                                List<Integer> partitionIdList,
                                                List<String> storeNames) {
        List<String> stores = storeNames;
        if(stores == null) {
            stores = Lists.newArrayList();
            List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                                   .getValue();
            for(StoreDefinition storeDefinition: storeDefinitionList) {
                stores.add(storeDefinition.getName());
            }
        }

        for(String store: stores) {
            System.out.println("Deleting partitions " + Joiner.on(", ").join(partitionIdList)
                               + " of " + store);
            adminClient.deletePartitions(nodeId, store, partitionIdList, null);
        }
    }

    private static void executeQueryKeys(final Integer nodeId,
                                         AdminClient adminClient,
                                         List<String> storeNames,
                                         List<String> keys) throws IOException {
        Serializer<String> serializer = new StringSerializer();
        List<ByteArray> listKeys = new ArrayList<ByteArray>();
        for(String key: keys) {
            listKeys.add(new ByteArray(serializer.toBytes(key)));
        }
        for(final String storeName: storeNames) {
            final Iterator<Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>>> iterator = adminClient.queryKeys(nodeId.intValue(),
                                                                                                                       storeName,
                                                                                                                       listKeys.iterator());
            List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                                   .getValue();
            StoreDefinition storeDefinition = null;
            for(StoreDefinition storeDef: storeDefinitionList) {
                if(storeDef.getName().equals(storeName))
                    storeDefinition = storeDef;
            }

            // k-v serializer
            SerializerDefinition keySerializerDef = storeDefinition.getKeySerializer();
            SerializerDefinition valueSerializerDef = storeDefinition.getValueSerializer();
            SerializerFactory serializerFactory = new DefaultSerializerFactory();
            @SuppressWarnings("unchecked")
            final Serializer<Object> keySerializer = (Serializer<Object>) serializerFactory.getSerializer(keySerializerDef);
            @SuppressWarnings("unchecked")
            final Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(valueSerializerDef);

            // compression strategy
            final CompressionStrategy keyCompressionStrategy;
            final CompressionStrategy valueCompressionStrategy;
            if(keySerializerDef != null && keySerializerDef.hasCompression()) {
                keyCompressionStrategy = new CompressionStrategyFactory().get(keySerializerDef.getCompression());
            } else {
                keyCompressionStrategy = null;
            }
            if(valueSerializerDef != null && valueSerializerDef.hasCompression()) {
                valueCompressionStrategy = new CompressionStrategyFactory().get(valueSerializerDef.getCompression());
            } else {
                valueCompressionStrategy = null;
            }

            // write to stdout
            writeAscii(null, new Writable() {

                @Override
                public void writeTo(BufferedWriter out) throws IOException {
                    final StringWriter stringWriter = new StringWriter();
                    final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);
                    stringWriter.write("Querying keys in node " + nodeId + " of " + storeName
                                       + "\n");

                    while(iterator.hasNext()) {
                        Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>> kvPair = iterator.next();
                        // unserialize and write key
                        byte[] keyBytes = kvPair.getFirst().get();
                        Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                                  : keyCompressionStrategy.inflate(keyBytes));
                        generator.writeObject(keyObject);

                        // iterate through, unserialize and write values
                        List<Versioned<byte[]>> values = kvPair.getSecond().getFirst();
                        if(values != null) {
                            if(values.size() == 0) {
                                stringWriter.write(", null");
                            }
                            for(Versioned<byte[]> versioned: values) {
                                VectorClock version = (VectorClock) versioned.getVersion();
                                byte[] valueBytes = versioned.getValue();
                                Object valueObject = valueSerializer.toObject((null == valueCompressionStrategy) ? valueBytes
                                                                                                                : valueCompressionStrategy.inflate(valueBytes));

                                stringWriter.write(", ");
                                stringWriter.write(version.toString());
                                stringWriter.write('[');
                                stringWriter.write(new Date(version.getTimestamp()).toString());
                                stringWriter.write(']');
                                generator.writeObject(valueObject);
                            }
                        } else {
                            stringWriter.write(", null");
                        }
                        // write out exception
                        if(kvPair.getSecond().getSecond() != null) {
                            stringWriter.write(", ");
                            stringWriter.write(kvPair.getSecond().getSecond().toString());
                        }

                        StringBuffer buf = stringWriter.getBuffer();
                        if(buf.charAt(0) == ' ') {
                            buf.setCharAt(0, '\n');
                        }
                        out.write(buf.toString());
                        buf.setLength(0);
                    }
                    out.write('\n');
                }
            });
        }
    }
}

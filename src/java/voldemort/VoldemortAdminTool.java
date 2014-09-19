/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.StoreRoutingPlan;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.StringSerializer;
import voldemort.serialization.json.JsonReader;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.CmdUtils;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sleepycat.persist.StoreNotFoundException;

/**
 * Provides a command line interface to the
 * {@link voldemort.client.protocol.admin.AdminClient}. This is the old
 * Voldemort Admin Tool. Please use the new one:
 * voldemort.tools.admin.VAdminTool
 */
@Deprecated
public class VoldemortAdminTool {

    private static final String ALL_METADATA = "all";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        // This is a generic argument that should be eventually supported by all
        // RW operations.
        // If you omit this argument the operation will be executed in a "batch"
        // mode which is useful for scripting
        // Otherwise you will be presented with a summary of changes and with a
        // Y/N prompt
        parser.accepts("auto", "[OPTIONAL] enable auto/batch mode");
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
        parser.accepts("nodes", "list of nodes")
              .withRequiredArg()
              .describedAs("nodes")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
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
                               + " | " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + " | "
                               + MetadataStore.SERVER_STATE_KEY + " ]")
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
                               + " | " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + " | "
                               + MetadataStore.REBALANCING_STEAL_INFO + " ]")
              .withRequiredArg()
              .describedAs("metadata-key")
              .ofType(String.class);
        parser.accepts("set-metadata-value",
                       "The value for the set-metadata [ " + MetadataStore.CLUSTER_KEY + " | "
                               + MetadataStore.STORES_KEY + ", "
                               + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + ", "
                               + MetadataStore.REBALANCING_STEAL_INFO
                               + " ] - xml file location, [ " + MetadataStore.SERVER_STATE_KEY
                               + " ] - " + MetadataStore.VoldemortState.NORMAL_SERVER + ","
                               + MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER)
              .withRequiredArg()
              .describedAs("metadata-value")
              .ofType(String.class);
        parser.accepts("update-store-defs",
                       "Update the ["
                               + MetadataStore.STORES_KEY
                               + "] with the new value for only the specified stores in update-value.");
        parser.accepts("update-store-value",
                       "The value for update-store-defs ] - xml file location")
              .withRequiredArg()
              .describedAs("stores-xml-value")
              .ofType(String.class);
        parser.accepts("set-metadata-pair",
                       "Atomic setting of metadata pair [ " + MetadataStore.CLUSTER_KEY + " & "
                               + MetadataStore.STORES_KEY + " ]")
              .withRequiredArg()
              .describedAs("metadata-keys-pair")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("set-metadata-value-pair",
                       "The value for the set-metadata pair [ " + MetadataStore.CLUSTER_KEY + " & "
                               + MetadataStore.STORES_KEY + " ]")
              .withRequiredArg()
              .describedAs("metadata-value-pair")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
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
        parser.accepts("prune-job", "Prune versioned put data, after rebalancing");
        parser.accepts("purge-slops",
                       "Purge the slop stores selectively, based on nodeId or zoneId");
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
        parser.accepts("query-key", "Get values of a key on specific node")
              .withRequiredArg()
              .describedAs("query-key")
              .ofType(String.class);
        parser.accepts("query-key-format", "Format of the query key. Can be one of [hex|readable]")
              .withRequiredArg()
              .describedAs("key-format")
              .ofType(String.class);
        parser.accepts("show-routing-plan", "Routing plan of the specified keys")
              .withRequiredArg()
              .describedAs("keys-to-be-routed")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("mirror-from-url", "Cluster url to mirror data from")
              .withRequiredArg()
              .describedAs("mirror-cluster-bootstrap-url")
              .ofType(String.class);
        parser.accepts("mirror-node", "Node id in the mirror cluster to mirror from")
              .withRequiredArg()
              .describedAs("id-of-mirror-node")
              .ofType(Integer.class);
        parser.accepts("fetch-orphaned", "Fetch any orphaned keys/entries in the node");
        parser.accepts("set-quota", "Enforce some quota on the servers")
              .withRequiredArg()
              .describedAs("quota-type")
              .ofType(String.class);
        parser.accepts("quota-value", "Value of the quota enforced on the servers")
              .withRequiredArg()
              .describedAs("quota-value")
              .ofType(String.class);
        parser.accepts("unset-quota", "Remove some quota already enforced on the servers")
              .withRequiredArg()
              .describedAs("quota-type")
              .ofType(String.class);
        // TODO add a way to retrieve all quotas for a given store.
        parser.accepts("get-quota", "Retrieve some quota already enforced on the servers")
              .withRequiredArg()
              .describedAs("quota-type")
              .ofType(String.class);

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            printHelp(System.out, parser);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "url", "node");
        if(missing.size() > 0) {
            // Not the most elegant way to do this
            // basically check if only "node" is missing for these set of
            // options; all these can live without explicit node ids
            if(!(missing.equals(ImmutableSet.of("node"))
                 && (options.has("add-stores") || options.has("delete-store")
                     || options.has("ro-metadata") || options.has("set-metadata")
                     || options.has("update-store-defs") || options.has("set-metadata-pair")
                     || options.has("get-metadata") || options.has("check-metadata"))
                 || options.has("truncate") || options.has("clear-rebalancing-metadata")
                 || options.has("async") || options.has("native-backup") || options.has("rollback")
                 || options.has("verify-metadata-version") || options.has("reserve-memory")
                 || options.has("purge-slops") || options.has("show-routing-plan")
                 || options.has("query-key") || options.has("set-quota")
                 || options.has("unset-quota") || options.has("get-quota"))) {
                System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
                printHelp(System.err, parser);
                System.exit(1);
            }
        }

        try {
            String url = (String) options.valueOf("url");
            Integer nodeId = CmdUtils.valueOf(options, "node", -1);
            int parallelism = CmdUtils.valueOf(options, "restore", 5);
            Integer zoneId = CmdUtils.valueOf(options, "zone", -1);

            AdminClient adminClient = new AdminClient(url,
                                                      new AdminClientConfig(),
                                                      new ClientConfig());

            List<String> storeNames = null;
            if(options.has("store") && options.has("stores")) {
                throw new VoldemortException("Must not specify both --stores and --store options");
            } else if(options.has("stores")) {
                storeNames = (List<String>) options.valuesOf("stores");
            } else if(options.has("store")) {
                storeNames = Arrays.asList((String) options.valueOf("store"));
            }

            String outputDir = null;
            if(options.has("outdir")) {
                outputDir = (String) options.valueOf("outdir");
            }

            if(options.has("add-stores")) {
                String storesXml = (String) options.valueOf("add-stores");
                executeAddStores(adminClient, storesXml, nodeId);
            } else if(options.has("async")) {
                String asyncKey = (String) options.valueOf("async");
                List<Integer> asyncIds = null;
                if(options.hasArgument("async-id"))
                    asyncIds = (List<Integer>) options.valuesOf("async-id");
                executeAsync(nodeId, adminClient, asyncKey, asyncIds);
            } else if(options.has("check-metadata")) {
                String metadataKey = (String) options.valueOf("check-metadata");
                executeCheckMetadata(adminClient, metadataKey);
            } else if(options.has("delete-partitions")) {
                System.out.println("Starting delete-partitions");
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("delete-partitions");
                executeDeletePartitions(nodeId, adminClient, partitionIdList, storeNames);
                System.out.println("Finished delete-partitions");
            } else if(options.has("ro-metadata")) {
                String type = (String) options.valueOf("ro-metadata");
                executeROMetadata(nodeId, adminClient, storeNames, type);
            } else if(options.has("reserve-memory")) {
                if(!options.has("stores")) {
                    Utils.croak("Specify the list of stores to reserve memory");
                }
                long reserveMB = (Long) options.valueOf("reserve-memory");
                adminClient.quotaMgmtOps.reserveMemory(nodeId, storeNames, reserveMB);
            } else if(options.has("get-metadata")) {
                String metadataKey = ALL_METADATA;
                if(options.hasArgument("get-metadata")) {
                    metadataKey = (String) options.valueOf("get-metadata");
                }
                executeGetMetadata(nodeId, adminClient, metadataKey, outputDir);
            } else if(options.has("mirror-from-url")) {
                if(!options.has("mirror-node")) {
                    Utils.croak("Specify the mirror node to fetch from");
                }

                if(nodeId == -1) {
                    System.err.println("Cannot run mirroring without node id");
                    System.exit(1);
                }
                Integer mirrorNodeId = CmdUtils.valueOf(options, "mirror-node", -1);
                if(mirrorNodeId == -1) {
                    System.err.println("Cannot run mirroring without mirror node id");
                    System.exit(1);
                }
                adminClient.restoreOps.mirrorData(nodeId,
                                                  mirrorNodeId,
                                                  (String) options.valueOf("mirror-from-url"),
                                                  storeNames);
            } else if(options.has("clear-rebalancing-metadata")) {
                executeClearRebalancing(nodeId, adminClient);
            } else if(options.has("prune-job")) {
                if(storeNames == null) {
                    Utils.croak("Must specify --stores to run the prune job");
                }
                executePruneJob(nodeId, adminClient, storeNames);
            } else if(options.has("fetch-keys")) {
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
                                 useAscii,
                                 options.has("fetch-orphaned"));
            } else if(options.has("repair-job")) {
                executeRepairJob(nodeId, adminClient);
            } else if(options.has("set-metadata-pair")) {
                List<String> metadataKeyPair = (List<String>) options.valuesOf("set-metadata-pair");
                if(metadataKeyPair.size() != 2) {
                    throw new VoldemortException("Missing set-metadata-pair keys (only two keys are needed and allowed)");
                }
                if(!options.has("set-metadata-value-pair")) {
                    throw new VoldemortException("Missing set-metadata-value-pair");
                } else {

                    List<String> metadataValuePair = (List<String>) options.valuesOf("set-metadata-value-pair");
                    if(metadataValuePair.size() != 2) {
                        throw new VoldemortException("Missing set-metadata--value-pair values (only two values are needed and allowed)");
                    }
                    if(metadataKeyPair.contains(MetadataStore.CLUSTER_KEY)
                       && metadataKeyPair.contains(MetadataStore.STORES_KEY)) {
                        ClusterMapper clusterMapper = new ClusterMapper();
                        StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
                        // original metadata
                        Integer nodeIdToGetStoreXMLFrom = nodeId;
                        if(nodeId < 0) {
                            Collection<Node> nodes = adminClient.getAdminClientCluster().getNodes();
                            if(nodes.isEmpty()) {
                                throw new VoldemortException("No nodes in this cluster");
                            } else {
                                nodeIdToGetStoreXMLFrom = nodes.iterator().next().getId();
                            }
                        }
                        Versioned<String> storesXML = adminClient.metadataMgmtOps.getRemoteMetadata(nodeIdToGetStoreXMLFrom,
                                                                                                    MetadataStore.STORES_KEY);
                        List<StoreDefinition> oldStoreDefs = storeDefsMapper.readStoreList(new StringReader(storesXML.getValue()));

                        String clusterXMLPath = metadataValuePair.get(metadataKeyPair.indexOf(MetadataStore.CLUSTER_KEY));
                        clusterXMLPath = clusterXMLPath.replace("~",
                                                                System.getProperty("user.home"));
                        if(!Utils.isReadableFile(clusterXMLPath))
                            throw new VoldemortException("Cluster xml file path incorrect");
                        Cluster cluster = clusterMapper.readCluster(new File(clusterXMLPath));

                        String storesXMLPath = metadataValuePair.get(metadataKeyPair.indexOf(MetadataStore.STORES_KEY));
                        storesXMLPath = storesXMLPath.replace("~", System.getProperty("user.home"));
                        if(!Utils.isReadableFile(storesXMLPath))
                            throw new VoldemortException("Stores definition xml file path incorrect");
                        List<StoreDefinition> newStoreDefs = storeDefsMapper.readStoreList(new File(storesXMLPath));
                        StoreDefinitionUtils.validateSchemasAsNeeded(newStoreDefs);

                        executeSetMetadataPair(nodeId,
                                               adminClient,
                                               MetadataStore.CLUSTER_KEY,
                                               clusterMapper.writeCluster(cluster),
                                               MetadataStore.STORES_KEY,
                                               storeDefsMapper.writeStoreList(newStoreDefs));
                        executeUpdateMetadataVersionsOnStores(adminClient,
                                                              oldStoreDefs,
                                                              newStoreDefs);
                    } else {
                        throw new VoldemortException("set-metadata-pair keys should be <cluster.xml, stores.xml>");
                    }
                }
            } else if(options.has("set-metadata")) {

                String metadataKey = (String) options.valueOf("set-metadata");
                if(!options.has("set-metadata-value")) {
                    throw new VoldemortException("Missing set-metadata-value");
                } else {
                    String metadataValue = (String) options.valueOf("set-metadata-value");
                    if(metadataKey.compareTo(MetadataStore.CLUSTER_KEY) == 0
                       || metadataKey.compareTo(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML) == 0) {
                        if(!Utils.isReadableFile(metadataValue))
                            throw new VoldemortException("Cluster xml file path incorrect");
                        ClusterMapper mapper = new ClusterMapper();
                        Cluster newCluster = mapper.readCluster(new File(metadataValue));
                        if(options.has("auto")) {
                            executeSetMetadata(nodeId,
                                               adminClient,
                                               metadataKey,
                                               mapper.writeCluster(newCluster));
                        } else {
                            if(confirmMetadataUpdate(nodeId,
                                                     adminClient,
                                                     mapper.writeCluster(newCluster))) {
                                executeSetMetadata(nodeId,
                                                   adminClient,
                                                   metadataKey,
                                                   mapper.writeCluster(newCluster));
                            } else {
                                System.out.println("New metadata has not been set");
                            }
                        }
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
                        List<StoreDefinition> newStoreDefs = mapper.readStoreList(new File(metadataValue));
                        StoreDefinitionUtils.validateSchemasAsNeeded(newStoreDefs);

                        // original metadata
                        Integer nodeIdToGetStoreXMLFrom = nodeId;
                        if(nodeId < 0) {
                            Collection<Node> nodes = adminClient.getAdminClientCluster().getNodes();
                            if(nodes.isEmpty()) {
                                throw new VoldemortException("No nodes in this cluster");
                            } else {
                                nodeIdToGetStoreXMLFrom = nodes.iterator().next().getId();
                            }
                        }

                        Versioned<String> storesXML = adminClient.metadataMgmtOps.getRemoteMetadata(nodeIdToGetStoreXMLFrom,
                                                                                                    MetadataStore.STORES_KEY);

                        List<StoreDefinition> oldStoreDefs = mapper.readStoreList(new StringReader(storesXML.getValue()));
                        if(options.has("auto")) {
                            executeSetMetadata(nodeId,
                                               adminClient,
                                               MetadataStore.STORES_KEY,
                                               mapper.writeStoreList(newStoreDefs));
                            executeUpdateMetadataVersionsOnStores(adminClient,
                                                                  oldStoreDefs,
                                                                  newStoreDefs);
                        } else {
                            if(confirmMetadataUpdate(nodeId, adminClient, storesXML.getValue())) {
                                executeSetMetadata(nodeId,
                                                   adminClient,
                                                   MetadataStore.STORES_KEY,
                                                   mapper.writeStoreList(newStoreDefs));
                                if(nodeId >= 0) {
                                    System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                                       + "although this set-metadata oprations only goes to node "
                                                       + nodeId);
                                }
                                executeUpdateMetadataVersionsOnStores(adminClient,
                                                                      oldStoreDefs,
                                                                      newStoreDefs);
                            } else {
                                System.out.println("New metadata has not been set");
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
            } else if(options.has("update-store-defs")) {
                if(!options.has("update-store-value")) {
                    throw new VoldemortException("Missing update-store-value for update-store-defs");
                } else {
                    String storesXmlValue = (String) options.valueOf("update-store-value");

                    if(!Utils.isReadableFile(storesXmlValue))
                        throw new VoldemortException("Stores definition xml file path incorrect");

                    StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
                    List<StoreDefinition> newStoreDefs = mapper.readStoreList(new File(storesXmlValue));
                    StoreDefinitionUtils.validateSchemasAsNeeded(newStoreDefs);

                    if(options.has("auto")) {
                        executeUpdateStoreDefinitions(nodeId, adminClient, newStoreDefs);
                    } else {
                        if(confirmMetadataUpdate(nodeId, adminClient, newStoreDefs)) {
                            executeUpdateStoreDefinitions(nodeId, adminClient, newStoreDefs);
                            if(nodeId >= 0) {
                                System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                                   + "although this set-metadata oprations only goes to node "
                                                   + nodeId);
                            }
                        } else {
                            System.out.println("New metadata has not been set");
                        }
                    }
                    System.out.println("The store definitions have been successfully updated.");
                }
            } else if(options.has("native-backup")) {
                if(!options.has("backup-dir")) {
                    Utils.croak("A backup directory must be specified with backup-dir option");
                }

                String backupDir = (String) options.valueOf("backup-dir");
                String storeName = (String) options.valueOf("native-backup");
                int timeout = CmdUtils.valueOf(options, "backup-timeout", 30);
                adminClient.storeMntOps.nativeBackup(nodeId,
                                                     storeName,
                                                     backupDir,
                                                     timeout,
                                                     options.has("backup-verify"),
                                                     options.has("backup-incremental"));
            } else if(options.has("rollback")) {
                if(!options.has("version")) {
                    Utils.croak("A read-only push version must be specified with rollback option");
                }
                String storeName = (String) options.valueOf("rollback");
                long pushVersion = (Long) options.valueOf("version");
                executeRollback(nodeId, storeName, pushVersion, adminClient);
            } else if(options.has("query-key")) {
                String key = (String) options.valueOf("query-key");
                String keyFormat = (String) options.valueOf("query-key-format");
                if(keyFormat == null) {
                    keyFormat = "hex";
                }
                if(!keyFormat.equals("hex") && !keyFormat.equals("readable")) {
                    throw new VoldemortException("--query-key-format must be hex or readable");
                }
                executeQueryKey(nodeId, adminClient, storeNames, key, keyFormat);
            } else if(options.has("restore")) {
                if(nodeId == -1) {
                    System.err.println("Cannot run restore without node id");
                    System.exit(1);
                }
                System.out.println("Starting restore");
                adminClient.restoreOps.restoreDataFromReplications(nodeId, parallelism, zoneId);
                System.out.println("Finished restore");
            } else if(options.has("delete-store")) {
                String storeName = (String) options.valueOf("delete-store");
                executeDeleteStore(adminClient, storeName, nodeId);
            } else if(options.has("truncate")) {
                String storeName = (String) options.valueOf("truncate");
                executeTruncateStore(nodeId, adminClient, storeName);
            } else if(options.has("update-entries")) {
                String inputDir = (String) options.valueOf("update-entries");
                executeUpdateEntries(nodeId, adminClient, storeNames, inputDir);
            } else if(options.has("fetch-entries")) {
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
                                    useAscii,
                                    options.has("fetch-orphaned"));
            } else if(options.has("purge-slops")) {
                List<Integer> nodesToPurge = null;
                if(options.has("nodes")) {
                    nodesToPurge = (List<Integer>) options.valuesOf("nodes");
                }
                if(nodesToPurge == null && zoneId == -1 && storeNames == null) {
                    Utils.croak("Must specify atleast one of --nodes, --zone-id or --stores with --purge-slops");
                }
                executePurgeSlops(adminClient, nodesToPurge, zoneId, storeNames);
            } else if(options.has("synchronize-metadata-version")) {
                synchronizeMetadataVersion(adminClient, nodeId);
            } else if(options.has("verify-metadata-version")) {
                checkMetadataVersion(adminClient);
            } else if(options.has("show-routing-plan")) {
                if(!options.has("store")) {
                    Utils.croak("Must specify the store the keys belong to using --store ");
                }
                String storeName = (String) options.valueOf("store");
                List<String> keysToRoute = (List<String>) options.valuesOf("show-routing-plan");
                if(keysToRoute == null || keysToRoute.size() == 0) {
                    Utils.croak("Must specify comma separated keys list in hex format");
                }
                executeShowRoutingPlan(adminClient, storeName, keysToRoute);

            } else if(options.has("set-quota")) {
                String quotaType = (String) options.valueOf("set-quota");
                Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
                if(!validQuotaTypes.contains(quotaType)) {
                    Utils.croak("Specify a valid quota type from :" + validQuotaTypes);
                }
                if(!options.has("store")) {
                    Utils.croak("Must specify the store to enforce the quota on. ");
                }
                if(!options.has("quota-value")) {
                    Utils.croak("Must specify the value of the quota being set");
                }
                String storeName = (String) options.valueOf("store");
                String quotaValue = (String) options.valueOf("quota-value");
                executeSetQuota(adminClient, storeName, quotaType, quotaValue);

            } else if(options.has("unset-quota")) {
                String quotaType = (String) options.valueOf("unset-quota");
                Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
                if(!validQuotaTypes.contains(quotaType)) {
                    Utils.croak("Specify a valid quota type from :" + validQuotaTypes);
                }
                if(!options.has("store")) {
                    Utils.croak("Must specify the store to enforce the quota on. ");
                }
                String storeName = (String) options.valueOf("store");
                executeUnsetQuota(adminClient, storeName, quotaType);
            } else if(options.has("get-quota")) {
                String quotaType = (String) options.valueOf("get-quota");
                Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
                if(!validQuotaTypes.contains(quotaType)) {
                    Utils.croak("Specify a valid quota type from :" + validQuotaTypes);
                }
                if(!options.has("store")) {
                    Utils.croak("Must specify the store to enforce the quota on. ");
                }
                String storeName = (String) options.valueOf("store");
                executeGetQuota(adminClient, storeName, quotaType);
            } else {

                Utils.croak("At least one of (delete-partitions, restore, add-node, fetch-entries, "
                            + "fetch-keys, add-stores, delete-store, update-entries, get-metadata, ro-metadata, "
                            + "set-metadata, check-metadata, clear-rebalancing-metadata, async, "
                            + "repair-job, native-backup, rollback, reserve-memory, mirror-url,"
                            + " verify-metadata-version, prune-job, purge-slops) must be specified");
            }
        } catch(Exception e) {
            e.printStackTrace();
            Utils.croak(e.getMessage());
        }
    }

    private static List<StoreDefinition> getStoreDefinitions(AdminClient adminClient, int nodeId) {
        Versioned<List<StoreDefinition>> storeDefs = null;
        if(nodeId >= 0) {
            storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId);
        } else {
            storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList();
        }
        return storeDefs.getValue();
    }

    private static void executeUpdateStoreDefinitions(Integer nodeId,
                                                      AdminClient adminClient,
                                                      List<StoreDefinition> storesList) {
        if(nodeId < 0) {
            adminClient.metadataMgmtOps.updateRemoteStoreDefList(storesList);
        } else {
            adminClient.metadataMgmtOps.updateRemoteStoreDefList(nodeId, storesList);
        }

    }

    private static String getMetadataVersionsForNode(AdminClient adminClient, int nodeId) {
        List<Integer> partitionIdList = Lists.newArrayList();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            partitionIdList.addAll(node.getPartitionIds());
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.bulkFetchOps.fetchEntries(nodeId,
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
            adminClient.metadataMgmtOps.setMetadataversion(props);
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
                adminClient.readonlyOps.rollbackStore(node.getId(), storeName, pushVersion);
            }
        } else {
            adminClient.readonlyOps.rollbackStore(nodeId, storeName, pushVersion);
        }
    }

    private static void executeRepairJob(Integer nodeId, AdminClient adminClient) {
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                adminClient.storeMntOps.repairJob(node.getId());
            }
        } else {
            adminClient.storeMntOps.repairJob(nodeId);
        }
    }

    private static void executePruneJob(Integer nodeId, AdminClient adminClient, List<String> stores) {
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                adminClient.storeMntOps.pruneJob(node.getId(), stores);
            }
        } else {
            adminClient.storeMntOps.pruneJob(nodeId, stores);
        }
    }

    private static void executePurgeSlops(AdminClient adminClient,
                                          List<Integer> nodesToPurge,
                                          Integer zoneToPurge,
                                          List<String> storesToPurge) {
        adminClient.storeMntOps.slopPurgeJob(nodesToPurge, zoneToPurge, storesToPurge);
    }

    private static void executeSetQuota(AdminClient adminClient,
                                        String storeName,
                                        String quotaType,
                                        String quotaValue) {
        if(!adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
            Utils.croak("Store " + storeName + " not in cluster.");
        }

        adminClient.quotaMgmtOps.setQuota(storeName, quotaType, quotaValue);
    }

    private static void executeUnsetQuota(AdminClient adminClient,
                                          String storeName,
                                          String quotaType) {
        if(!adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
            Utils.croak("Store " + storeName + " not in cluster.");
        }

        adminClient.quotaMgmtOps.unsetQuota(storeName, quotaType);
    }

    private static void executeGetQuota(AdminClient adminClient, String storeName, String quotaType) {
        if(!adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
            Utils.croak("Store " + storeName + " not in cluster.");
        }

        Versioned<String> quotaVal = adminClient.quotaMgmtOps.getQuota(storeName, quotaType);
        if(quotaVal == null) {
            System.out.println("No quota set for " + quotaType + " on store " + storeName);
        } else {
            System.out.println("Quota value  for " + quotaType + " on store " + storeName + " : "
                               + quotaVal.getValue());
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
                       + MetadataStore.STORES_KEY + ", "
                       + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + ", "
                       + MetadataStore.REBALANCING_STEAL_INFO
                       + "] --set-metadata-value [metadata-value] --url [url]");
        stream.println("\t6) Set metadata for a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --set-metadata ["
                       + MetadataStore.CLUSTER_KEY + ", " + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.STORES_KEY + ", "
                       + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + ", "
                       + MetadataStore.REBALANCING_STEAL_INFO
                       + "] --set-metadata-value [metadata-value] --url [url] --node [node-id]");
        stream.println("\t7) Update store definitions on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --update-store-defs --update-store-value [Updated store definitions XML] --url [url]");
        stream.println("\t8) Check if metadata is same on all nodes");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --check-metadata ["
                       + MetadataStore.CLUSTER_KEY + ", " + MetadataStore.SERVER_STATE_KEY + ", "
                       + MetadataStore.STORES_KEY + "] --url [url]");
        stream.println("\t9) Clear rebalancing metadata [" + MetadataStore.SERVER_STATE_KEY + ", "
                       + ", " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + ", "
                       + MetadataStore.REBALANCING_STEAL_INFO + "] on all node ");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --clear-rebalancing-metadata --url [url]");
        stream.println("\t10) Clear rebalancing metadata [" + MetadataStore.SERVER_STATE_KEY + ", "
                       + ", " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML + ", "
                       + MetadataStore.REBALANCING_STEAL_INFO + "] on a particular node ");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --clear-rebalancing-metadata --url [url] --node [node-id]");
        stream.println("\t11) View detailed routing information for a given set of keys.");
        stream.println("bin/voldemort-admin-tool.sh --url <url> --show-routing-plan key1,key2,.. --store <store-name>");
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
        stream.println("\t10.a) Query stores for a set of keys on a specific node, key is in hex format");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --query-key [hex_key] --query-key-format hex --url [url] --node [node-id] --stores [comma-separated list of store names]");
        stream.println("\t11.a) Query stores for a set of keys on all nodes, key is in hex format");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --query-key [hex_key] --query-key-format hex --url [url] --stores [comma-separated list of store names]");
        stream.println("\t12.a) Query stores for a set of keys on a specific node, in readable format. JSON string must be between quotation marks with inside quotation marks escaped");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --query-key [readable_key] --query-key-format readable --url [url] --node [node-id] --stores [comma-separated list of store names]");
        stream.println("\t13) Mirror data from another voldemort server (possibly in another cluster) for specified stores");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --mirror-from-url [bootstrap url to mirror from] --mirror-node [node to mirror from] --url [url] --node [node-id] --stores [comma-separated-list-of-store-names]");
        stream.println("\t14) Mirror data from another voldemort server (possibly in another cluster) for all stores in current cluster");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --mirror-from-url [bootstrap url to mirror from] --mirror-node [node to mirror from] --url [url] --node [node-id]");
        stream.println("\t15) Fetch all orphaned keys on a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-keys --url [url] --node [node-id] --fetch-orphaned");
        stream.println("\t16) Fetch all orphaned entries on a particular node");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --fetch-entries --url [url] --node [node-id] --fetch-orphaned");
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
        stream.println("\t3) Clean a node after rebalancing is done");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --repair-job --url [url] --node [node-id]");
        stream.println("\t4) Backup bdb data natively");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --native-backup [store] --backup-dir [outdir] "
                       + "--backup-timeout [mins] [--backup-verify] [--backup-incremental] --url [url] --node [node-id]");
        stream.println("\t5) Rollback a read-only store to the specified push version");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --rollback [store-name] --url [url] --node [node-id] --version [version-num] ");
        stream.println("\t7) Prune data resulting from versioned puts, during rebalancing");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --prune-job --url [url] --node [node-id] --stores [stores_list]");
        stream.println("\t8) Purge slops based on criteria");
        stream.println("\t\t./bin/voldemort-admin-tool.sh --purge-slops --url [url] --nodes [destination-nodes-list] --stores [stores_list] --zone [destination-zone]");
        stream.println("\t9) Set Quota limits on the servers. One of "
                       + QuotaUtils.validQuotaTypes());
        stream.println("\t\t bin/voldemort-admin-tool.sh --url [url] --set-quota [quota-type] --quota-value [value] --store [store-name]");
        stream.println("\t10) Unset Quota limits on the servers. One of "
                       + QuotaUtils.validQuotaTypes());
        stream.println("\t\t bin/voldemort-admin-tool.sh --url [url] --unset-quota [quota-type] --store [store-name]");
        stream.println("\t11) Get Quota limits on the servers. One of "
                       + QuotaUtils.validQuotaTypes());
        stream.println("\t\t bin/voldemort-admin-tool.sh --url [url] --get-quota [quota-type] --store [store-name]");

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
                List<Integer> asyncIds = adminClient.rpcOps.getAsyncRequestList(currentNodeId);
                System.out.println("Async Job Ids on node " + currentNodeId + " : " + asyncIds);
                for(int asyncId: asyncIds) {
                    System.out.println("Async Job Id "
                                       + asyncId
                                       + " ] "
                                       + adminClient.rpcOps.getAsyncRequestStatus(currentNodeId,
                                                                                  asyncId));
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
                adminClient.rpcOps.stopAsyncRequest(nodeId, asyncId);
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
        System.out.println("Cleaning up " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML
                           + " to empty string");
        executeSetMetadata(nodeId, adminClient, MetadataStore.REBALANCING_SOURCE_CLUSTER_XML, "");
    }

    private static void addMetadataValue(Map<Object, List<String>> allValues,
                                         Object metadataValue,
                                         String node) {
        if(allValues.containsKey(metadataValue) == false) {
            allValues.put(metadataValue, new ArrayList<String>());
        }
        allValues.get(metadataValue).add(node);
    }

    private static Boolean checkDiagnostics(String keyName,
                                            Map<Object, List<String>> metadataValues,
                                            Collection<String> allNodeNames) {

        Collection<String> nodesInResult = new ArrayList<String>();
        Boolean checkResult = true;

        if(metadataValues.size() == 1) {
            Map.Entry<Object, List<String>> entry = metadataValues.entrySet().iterator().next();
            nodesInResult.addAll(entry.getValue());
        } else {
            // Some nodes have different set of data than the others.
            checkResult = false;
            int groupCount = 0;
            for(Map.Entry<Object, List<String>> entry: metadataValues.entrySet()) {
                groupCount++;
                System.err.println("Nodes with same value for " + keyName + ". Id :" + groupCount);
                nodesInResult.addAll(entry.getValue());
                for(String nodeName: entry.getValue()) {
                    System.err.println("Node " + nodeName);
                }
                System.out.println();
            }
        }

        // Some times when a store could be missing from one of the nodes
        // In that case the map will have only one value, but total number
        // of nodes will be lesser. The following code handles that.

        // removeAll modifies the list that is being called on. so create a copy
        Collection<String> nodesDiff = new ArrayList<String>(allNodeNames.size());
        nodesDiff.addAll(allNodeNames);
        nodesDiff.removeAll(nodesInResult);

        if(nodesDiff.size() > 0) {
            checkResult = false;
            for(String nodeName: nodesDiff) {
                System.err.println("key " + keyName + " is missing in the Node " + nodeName);
            }
        }
        return checkResult;
    }

    private static void executeCheckMetadata(AdminClient adminClient, String metadataKey) {

        Map<String, Map<Object, List<String>>> storeNodeValueMap = new HashMap<String, Map<Object, List<String>>>();
        Map<Object, List<String>> metadataNodeValueMap = new HashMap<Object, List<String>>();
        Collection<Node> allNodes = adminClient.getAdminClientCluster().getNodes();
        Collection<String> allNodeNames = new ArrayList<String>();

        Boolean checkResult = true;
        for(Node node: allNodes) {
            String nodeName = "Host '" + node.getHost() + "' : ID " + node.getId();
            allNodeNames.add(nodeName);

            System.out.println("processing " + nodeName);

            Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                        metadataKey);
            if(versioned == null || versioned.getValue() == null) {
                throw new VoldemortException("Value returned from node " + node.getId()
                                             + " was null");
            } else if(metadataKey.compareTo(MetadataStore.STORES_KEY) == 0) {
                List<StoreDefinition> storeDefinitions = new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue()));
                for(StoreDefinition storeDef: storeDefinitions) {
                    String storeName = storeDef.getName();
                    if(storeNodeValueMap.containsKey(storeName) == false) {
                        storeNodeValueMap.put(storeName, new HashMap<Object, List<String>>());
                    }
                    Map<Object, List<String>> storeDefMap = storeNodeValueMap.get(storeName);
                    addMetadataValue(storeDefMap, storeDef, nodeName);
                }
            } else {
                if(metadataKey.compareTo(MetadataStore.CLUSTER_KEY) == 0
                   || metadataKey.compareTo(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML) == 0) {
                    Cluster cluster = new ClusterMapper().readCluster(new StringReader(versioned.getValue()));
                    addMetadataValue(metadataNodeValueMap, cluster, nodeName);
                } else if(metadataKey.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                    VoldemortState voldemortStateValue = VoldemortState.valueOf(versioned.getValue());
                    addMetadataValue(metadataNodeValueMap, voldemortStateValue, nodeName);
                } else {
                    throw new VoldemortException("Incorrect metadata key");
                }

            }
        }

        if(metadataNodeValueMap.size() > 0) {
            checkResult &= checkDiagnostics(metadataKey, metadataNodeValueMap, allNodeNames);
        }

        if(storeNodeValueMap.size() > 0) {
            for(Map.Entry<String, Map<Object, List<String>>> storeNodeValueEntry: storeNodeValueMap.entrySet()) {
                String storeName = storeNodeValueEntry.getKey();
                Map<Object, List<String>> storeDefMap = storeNodeValueEntry.getValue();
                checkResult &= checkDiagnostics(storeName, storeDefMap, allNodeNames);
            }
        }

        System.out.println("metadata check : " + (checkResult ? "PASSED" : "FAILED"));
    }

    /*
     * Update <cluster.xml,stores.xml> pair atomically
     */
    public static void executeSetMetadataPair(Integer nodeId,
                                              AdminClient adminClient,
                                              String clusterKey,
                                              Object clusterValue,
                                              String storesKey,
                                              Object storesValue) {

        List<Integer> nodeIds = Lists.newArrayList();
        VectorClock updatedClusterVersion = null;
        VectorClock updatedStoresVersion = null;
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {

                nodeIds.add(node.getId());

                if(updatedClusterVersion == null && updatedStoresVersion == null) {
                    updatedClusterVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                        clusterKey)
                                                                                     .getVersion();

                    updatedStoresVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                       storesKey)
                                                                                    .getVersion();
                } else {
                    updatedClusterVersion = updatedClusterVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                                                    clusterKey)
                                                                                                                 .getVersion());

                    updatedStoresVersion = updatedStoresVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                                                  storesKey)
                                                                                                               .getVersion());
                }
            }
            // TODO: This will work for now but we should take a step back and
            // think about a uniform clock for the metadata values.
            updatedClusterVersion = updatedClusterVersion.incremented(0, System.currentTimeMillis());
            updatedStoresVersion = updatedStoresVersion.incremented(0, System.currentTimeMillis());

        } else {
            updatedClusterVersion = ((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                 clusterKey)
                                                                              .getVersion()).incremented(nodeId,
                                                                                                         System.currentTimeMillis());
            updatedStoresVersion = ((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                storesKey)
                                                                             .getVersion()).incremented(nodeId,
                                                                                                        System.currentTimeMillis());

            nodeIds.add(nodeId);
        }
        adminClient.metadataMgmtOps.updateRemoteMetadataPair(nodeIds,
                                                             clusterKey,
                                                             Versioned.value(clusterValue.toString(),
                                                                             updatedClusterVersion),
                                                             storesKey,
                                                             Versioned.value(storesValue.toString(),
                                                                             updatedStoresVersion));
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
                    updatedVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                 key)
                                                                              .getVersion();
                } else {
                    updatedVersion = updatedVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                                      key)
                                                                                                   .getVersion());
                }
            }

            // Bump up version on node 0
            updatedVersion = updatedVersion.incremented(0, System.currentTimeMillis());
        } else {
            Versioned<String> currentValue = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                           key);
            updatedVersion = ((VectorClock) currentValue.getVersion()).incremented(nodeId,
                                                                                   System.currentTimeMillis());
            nodeIds.add(nodeId);
        }
        adminClient.metadataMgmtOps.updateRemoteMetadata(nodeIds,
                                                         key,
                                                         Versioned.value(value.toString(),
                                                                         updatedVersion));
    }

    private static void executeUpdateMetadataVersionsOnStores(AdminClient adminClient,
                                                              List<StoreDefinition> oldStoreDefs,
                                                              List<StoreDefinition> newStoreDefs) {
        Set<String> storeNamesUnion = new HashSet<String>();
        Map<String, StoreDefinition> oldStoreDefinitionMap = new HashMap<String, StoreDefinition>();
        Map<String, StoreDefinition> newStoreDefinitionMap = new HashMap<String, StoreDefinition>();
        List<String> storesChanged = new ArrayList<String>();
        for(StoreDefinition storeDef: oldStoreDefs) {
            String storeName = storeDef.getName();
            storeNamesUnion.add(storeName);
            oldStoreDefinitionMap.put(storeName, storeDef);
        }
        for(StoreDefinition storeDef: newStoreDefs) {
            String storeName = storeDef.getName();
            storeNamesUnion.add(storeName);
            newStoreDefinitionMap.put(storeName, storeDef);
        }
        for(String storeName: storeNamesUnion) {
            StoreDefinition oldStoreDef = oldStoreDefinitionMap.get(storeName);
            StoreDefinition newStoreDef = newStoreDefinitionMap.get(storeName);
            if(oldStoreDef == null && newStoreDef != null || oldStoreDef != null
               && newStoreDef == null || oldStoreDef != null && newStoreDef != null
               && !oldStoreDef.equals(newStoreDef)) {
                storesChanged.add(storeName);
            }
        }
        System.out.println("Updating metadata version for the following stores: " + storesChanged);
        try {
            adminClient.metadataMgmtOps.updateMetadataversion(storesChanged);
        } catch(Exception e) {
            System.err.println("Error while updating metadata version for the specified store.");
        }
    }

    private static void executeROMetadata(Integer nodeId,
                                          AdminClient adminClient,
                                          List<String> storeNames,
                                          String type) {
        Map<String, Long> storeToValue = Maps.newHashMap();

        if(storeNames == null) {
            // Retrieve list of read-only stores
            storeNames = Lists.newArrayList();

            for(StoreDefinition storeDef: getStoreDefinitions(adminClient, nodeId)) {
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
                storeToValue = adminClient.readonlyOps.getROMaxVersion(currentNodeId, storeNames);
            } else if(type.compareTo("current") == 0) {
                storeToValue = adminClient.readonlyOps.getROCurrentVersion(currentNodeId,
                                                                           storeNames);
            } else if(type.compareTo("storage-format") == 0) {
                Map<String, String> storeToStorageFormat = adminClient.readonlyOps.getROStorageFormat(currentNodeId,
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

    public static void executeGetMetadata(Integer nodeId,
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
                    versioned = adminClient.metadataMgmtOps.getRemoteMetadata(currentNodeId, key);
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
            adminClient.storeMgmtOps.deleteStore(storeName);
        } else {
            adminClient.storeMgmtOps.deleteStore(storeName, nodeId);
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
            adminClient.storeMntOps.truncate(currentNodeId, storeName);
        }
    }

    private static void executeAddStores(AdminClient adminClient, String storesXml, int nodeId)
            throws IOException {
        List<StoreDefinition> storeDefinitionList = new StoreDefinitionsMapper().readStoreList(new File(storesXml));
        for(StoreDefinition storeDef: storeDefinitionList) {
            System.out.println("Adding " + storeDef.getName());
            if(-1 != nodeId)
                adminClient.storeMgmtOps.addStore(storeDef, nodeId);
            else
                adminClient.storeMgmtOps.addStore(storeDef);
        }
    }

    private static void executeFetchEntries(Integer nodeId,
                                            AdminClient adminClient,
                                            List<Integer> partitionIdList,
                                            String outputDir,
                                            List<String> storeNames,
                                            boolean useAscii,
                                            boolean fetchOrphaned) throws IOException {

        List<StoreDefinition> storeDefinitionList = getStoreDefinitions(adminClient, nodeId);
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
            }

            Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIteratorRef = null;
            if(fetchOrphaned) {
                System.out.println("Fetching orphaned entries of " + store);
                entriesIteratorRef = adminClient.bulkFetchOps.fetchOrphanedEntries(nodeId, store);
            } else {
                System.out.println("Fetching entries in partitions "
                                   + Joiner.on(", ").join(partitionIdList) + " of " + store);
                entriesIteratorRef = adminClient.bulkFetchOps.fetchEntries(nodeId,
                                                                           store,
                                                                           partitionIdList,
                                                                           null,
                                                                           false);
            }

            final Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = entriesIteratorRef;
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

                        while(entriesIterator.hasNext()) {
                            final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(out);
                            Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                            byte[] keyBytes = kvPair.getFirst().get();
                            byte[] valueBytes = kvPair.getSecond().getValue();
                            VectorClock version = (VectorClock) kvPair.getSecond().getVersion();

                            Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                                      : keyCompressionStrategy.inflate(keyBytes));
                            Object valueObject = valueSerializer.toObject((null == valueCompressionStrategy) ? valueBytes
                                                                                                            : valueCompressionStrategy.inflate(valueBytes));
                            if(keyObject instanceof GenericRecord) {
                                out.write(keyObject.toString());
                            } else {
                                generator.writeObject(keyObject);
                            }
                            out.write(' ' + version.toString() + ' ');
                            if(valueObject instanceof GenericRecord) {
                                out.write(valueObject.toString());
                            } else {
                                generator.writeObject(valueObject);
                            }
                            out.write('\n');
                        }
                    }
                });
            } else {
                writeBinary(outputFile, new Printable() {

                    @Override
                    public void printTo(DataOutputStream out) throws IOException {
                        while(entriesIterator.hasNext()) {
                            Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                            byte[] keyBytes = kvPair.getFirst().get();
                            VectorClock clock = ((VectorClock) kvPair.getSecond().getVersion());
                            byte[] valueBytes = kvPair.getSecond().getValue();

                            out.writeChars(ByteUtils.toHexString(keyBytes));
                            out.writeChars(",");
                            out.writeChars(clock.toString());
                            out.writeChars(",");
                            out.writeChars(ByteUtils.toHexString(valueBytes));
                            out.writeChars("\n");
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
        List<StoreDefinition> storeDefinitionList = getStoreDefinitions(adminClient, nodeId);
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
            adminClient.streamingOps.updateEntries(nodeId, storeName, iterator, null);
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
                                         boolean useAscii,
                                         boolean fetchOrphaned) throws IOException {
        List<StoreDefinition> storeDefinitionList = getStoreDefinitions(adminClient, nodeId);
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
            }

            Iterator<ByteArray> keyIteratorRef = null;
            if(fetchOrphaned) {
                System.out.println("Fetching orphaned keys  of " + store);
                keyIteratorRef = adminClient.bulkFetchOps.fetchOrphanedKeys(nodeId, store);
            } else {
                System.out.println("Fetching keys in partitions "
                                   + Joiner.on(", ").join(partitionIdList) + " of " + store);
                keyIteratorRef = adminClient.bulkFetchOps.fetchKeys(nodeId,
                                                                    store,
                                                                    partitionIdList,
                                                                    null,
                                                                    false);
            }
            File outputFile = null;
            if(directory != null) {
                outputFile = new File(directory, store + ".keys");
            }
            final Iterator<ByteArray> keyIterator = keyIteratorRef;
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

                        while(keyIterator.hasNext()) {
                            final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(out);

                            byte[] keyBytes = keyIterator.next().get();
                            Object keyObject = serializer.toObject((null == keysCompressionStrategy) ? keyBytes
                                                                                                    : keysCompressionStrategy.inflate(keyBytes));

                            if(keyObject instanceof GenericRecord) {
                                out.write(keyObject.toString());
                            } else {
                                generator.writeObject(keyObject);
                            }
                            out.write('\n');
                        }
                    }
                });
            } else {
                writeBinary(outputFile, new Printable() {

                    @Override
                    public void printTo(DataOutputStream out) throws IOException {
                        while(keyIterator.hasNext()) {
                            byte[] keyBytes = keyIterator.next().get();
                            out.writeChars(ByteUtils.toHexString(keyBytes) + "\n");
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
            List<StoreDefinition> storeDefinitionList = getStoreDefinitions(adminClient, nodeId);
            for(StoreDefinition storeDefinition: storeDefinitionList) {
                stores.add(storeDefinition.getName());
            }
        }

        for(String store: stores) {
            System.out.println("Deleting partitions " + Joiner.on(", ").join(partitionIdList)
                               + " of " + store);
            adminClient.storeMntOps.deletePartitions(nodeId, store, partitionIdList, null);
        }
    }

    private static void executeQueryKey(final Integer nodeId,
                                        AdminClient adminClient,
                                        List<String> storeNames,
                                        String keyString,
                                        String keyFormat) throws IOException {

        // decide queryingNode(s) for Key
        List<Integer> queryingNodes = new ArrayList<Integer>();
        if(nodeId < 0) { // means all nodes
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                queryingNodes.add(node.getId());
            }
        } else {
            queryingNodes.add(nodeId);
        }

        // get basic info
        List<StoreDefinition> storeDefinitionList = getStoreDefinitions(adminClient, nodeId);
        Map<String, StoreDefinition> storeDefinitions = new HashMap<String, StoreDefinition>();
        for(StoreDefinition storeDef: storeDefinitionList) {
            storeDefinitions.put(storeDef.getName(), storeDef);
        }

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));

        // iterate through stores
        for(final String storeName: storeNames) {
            // store definition
            StoreDefinition storeDefinition = storeDefinitions.get(storeName);
            if(storeDefinition == null) {
                throw new StoreNotFoundException("Store " + storeName + " not found");
            }

            out.write("STORE_NAME: " + storeDefinition.getName() + "\n");

            // k-v serializer
            final SerializerDefinition keySerializerDef = storeDefinition.getKeySerializer();
            final SerializerDefinition valueSerializerDef = storeDefinition.getValueSerializer();
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

            if(keyCompressionStrategy == null) {
                out.write("KEY_COMPRESSION_STRATEGY: None\n");
            } else {
                out.write("KEY_COMPRESSION_STRATEGY: " + keyCompressionStrategy.getType() + "\n");
            }
            out.write("KEY_SERIALIZER_NAME: " + keySerializerDef.getName() + "\n");
            for(Map.Entry<Integer, String> entry: keySerializerDef.getAllSchemaInfoVersions()
                                                                  .entrySet()) {
                out.write(String.format("KEY_SCHEMA VERSION=%d\n", entry.getKey()));
                out.write("====================================\n");
                out.write(entry.getValue());
                out.write("\n====================================\n");
            }
            out.write("\n");
            if(valueCompressionStrategy == null) {
                out.write("VALUE_COMPRESSION_STRATEGY: None\n");
            } else {
                out.write("VALUE_COMPRESSION_STRATEGY: " + valueCompressionStrategy.getType()
                          + "\n");
            }
            out.write("VALUE_SERIALIZER_NAME: " + valueSerializerDef.getName() + "\n");
            for(Map.Entry<Integer, String> entry: valueSerializerDef.getAllSchemaInfoVersions()
                                                                    .entrySet()) {
                out.write(String.format("VALUE_SCHEMA %d\n", entry.getKey()));
                out.write("====================================\n");
                out.write(entry.getValue());
                out.write("\n====================================\n");
            }
            out.write("\n");

            // although the streamingOps support multiple keys, we only query
            // one key here
            ByteArray key;
            try {
                if(keyFormat.equals("readable")) {
                    Object keyObject;
                    String keySerializerName = keySerializerDef.getName();
                    if(isAvroSchema(keySerializerName)) {
                        Schema keySchema = Schema.parse(keySerializerDef.getCurrentSchemaInfo());
                        JsonDecoder decoder = new JsonDecoder(keySchema, keyString);
                        GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(keySchema);
                        keyObject = datumReader.read(null, decoder);
                    } else if(keySerializerName.equals(DefaultSerializerFactory.JSON_SERIALIZER_TYPE_NAME)) {
                        JsonReader jsonReader = new JsonReader(new StringReader(keyString));
                        keyObject = jsonReader.read();
                    } else {
                        keyObject = keyString;
                    }

                    key = new ByteArray(keySerializer.toBytes(keyObject));
                } else {
                    key = new ByteArray(ByteUtils.fromHexString(keyString));
                }
            } catch(SerializationException se) {
                System.err.println("Error serializing key " + keyString);
                System.err.println("If this is a JSON key, you need to include escaped quotation marks in the command line if it is a string");
                se.printStackTrace();
                return;
            } catch(DecoderException de) {
                System.err.println("Error decoding key " + keyString);
                de.printStackTrace();
                return;
            } catch(IOException io) {
                System.err.println("Error parsing avro string " + keyString);
                io.printStackTrace();
                return;
            }

            boolean printedKey = false;
            // A Map<> could have been used instead of List<Entry<>> if
            // Versioned supported correct hash codes. Read the comment in
            // Versioned about the issue
            List<Entry<List<Versioned<byte[]>>, List<Integer>>> nodeValues = new ArrayList<Entry<List<Versioned<byte[]>>, List<Integer>>>();
            for(final Integer queryNodeId: queryingNodes) {
                Iterator<QueryKeyResult> iterator;
                iterator = adminClient.streamingOps.queryKeys(queryNodeId,
                                                              storeName,
                                                              Arrays.asList(key).iterator());
                final StringWriter stringWriter = new StringWriter();

                QueryKeyResult queryKeyResult = iterator.next();

                if(!printedKey) {
                    // de-serialize and write key
                    byte[] keyBytes = queryKeyResult.getKey().get();
                    Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                              : keyCompressionStrategy.inflate(keyBytes));

                    writeVoldKeyOrValueInternal(keyBytes,
                                                keySerializer,
                                                keyCompressionStrategy,
                                                "KEY",
                                                out);
                    printedKey = true;
                }

                // iterate through, de-serialize and write values
                if(queryKeyResult.hasValues() && queryKeyResult.getValues().size() > 0) {

                    int elementId = -1;
                    for(int i = 0; i < nodeValues.size(); i++) {
                        if(Objects.equal(nodeValues.get(i).getKey(), queryKeyResult.getValues())) {
                            elementId = i;
                            break;
                        }
                    }

                    if(elementId == -1) {
                        ArrayList<Integer> nodes = new ArrayList<Integer>();
                        nodes.add(queryNodeId);
                        nodeValues.add(new AbstractMap.SimpleEntry<List<Versioned<byte[]>>, List<Integer>>(queryKeyResult.getValues(),
                                                                                                           nodes));
                    } else {
                        nodeValues.get(elementId).getValue().add(queryNodeId);
                    }

                    out.write(String.format("\nQueried node %d on store %s\n",
                                            queryNodeId,
                                            storeName));

                    int versionCount = 0;

                    if(queryKeyResult.getValues().size() > 1) {
                        out.write("VALUE " + versionCount + "\n");
                    }

                    for(Versioned<byte[]> versioned: queryKeyResult.getValues()) {

                        // write version
                        VectorClock version = (VectorClock) versioned.getVersion();
                        out.write("VECTOR_CLOCK_BYTE: " + ByteUtils.toHexString(version.toBytes())
                                  + "\n");
                        out.write("VECTOR_CLOCK_TEXT: " + version.toString() + '['
                                  + new Date(version.getTimestamp()).toString() + "]\n");

                        // write value
                        byte[] valueBytes = versioned.getValue();
                        writeVoldKeyOrValueInternal(valueBytes,
                                                    valueSerializer,
                                                    valueCompressionStrategy,
                                                    "VALUE",
                                                    out);
                        versionCount++;
                    }
                } // If a node does not host a key, it returns invalidmetdata
                  // exception.
                else if(queryKeyResult.hasException()) {
                    boolean isInvalidMetadataException = queryKeyResult.getException() instanceof InvalidMetadataException;

                    // Print the exception if not InvalidMetadataException or
                    // you are querying only a single node.
                    if(!isInvalidMetadataException || queryingNodes.size() == 1) {
                        out.write(String.format("\nNode %d on store %s returned exception\n",
                                                queryNodeId,
                                                storeName));
                        out.write(queryKeyResult.getException().toString());
                        out.write("\n====================================\n");
                    }
                } else {
                    if(queryingNodes.size() == 1) {
                        out.write(String.format("\nNode %d on store %s returned NULL\n",
                                                queryNodeId,
                                                storeName));
                        out.write("\n====================================\n");
                    }
                }
                out.flush();
            }

            out.write("\n====================================\n");
            for(Map.Entry<List<Versioned<byte[]>>, List<Integer>> nodeValue: nodeValues) {
                out.write("Nodes with same Value "
                          + Arrays.toString(nodeValue.getValue().toArray()));
                out.write("\n====================================\n");
            }
            if(nodeValues.size() > 1) {
                out.write("\n*** Multiple (" + nodeValues.size()
                          + ") versions of key/value exist for the key ***\n");
            }
            out.flush();
        }
    }

    private static void writeVoldKeyOrValueInternal(byte[] input,
                                                    Serializer<Object> serializer,
                                                    CompressionStrategy compressionStrategy,
                                                    String prefix,
                                                    BufferedWriter out) throws IOException {
        out.write(prefix + "_BYTES\n====================================\n");
        out.write(ByteUtils.toHexString(input));
        out.write("\n====================================\n");
        try {
            Object inputObject = serializer.toObject((null == compressionStrategy) ? input
                                                                                  : compressionStrategy.inflate(input));

            out.write(prefix + "_TEXT\n====================================\n");
            if(inputObject instanceof GenericRecord) {
                out.write(inputObject.toString());
            } else {
                new JsonFactory(new ObjectMapper()).createJsonGenerator(out)
                                                   .writeObject(inputObject);
            }
            out.write("\n====================================\n\n");
        } catch(SerializationException e) {}

    }

    private static void executeShowRoutingPlan(AdminClient adminClient,
                                               String storeName,
                                               List<String> keyList) throws DecoderException {

        Cluster cluster = adminClient.getAdminClientCluster();
        List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList()
                                                                     .getValue();
        StoreDefinition storeDef = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                   storeName);
        StoreRoutingPlan routingPlan = new StoreRoutingPlan(cluster, storeDef);
        BaseStoreRoutingPlan bRoutingPlan = new BaseStoreRoutingPlan(cluster, storeDef);

        final int COLUMN_WIDTH = 30;

        for(String keyStr: keyList) {
            byte[] key = ByteUtils.fromHexString(keyStr);
            System.out.println("Key :" + keyStr);
            System.out.println("Replicating Partitions :"
                               + routingPlan.getReplicatingPartitionList(key));
            System.out.println("Replicating Nodes :");
            List<Integer> nodeList = routingPlan.getReplicationNodeList(routingPlan.getMasterPartitionId(key));
            for(int i = 0; i < nodeList.size(); i++) {
                System.out.println(nodeList.get(i) + "\t"
                                   + cluster.getNodeById(nodeList.get(i)).getHost());
            }

            System.out.println("Zone Nary information :");
            HashMap<Integer, Integer> zoneRepMap = storeDef.getZoneReplicationFactor();

            for(Zone zone: cluster.getZones()) {
                System.out.println("\tZone #" + zone.getId());
                int numReplicas = -1;
                if(zoneRepMap == null) {
                    // non zoned cluster
                    numReplicas = storeDef.getReplicationFactor();
                } else {
                    // zoned cluster
                    if(!zoneRepMap.containsKey(zone.getId())) {
                        Utils.croak("Repfactor for Zone " + zone.getId() + " not found in storedef");
                    }
                    numReplicas = zoneRepMap.get(zone.getId());
                }

                String FormatString = "%s %s %s\n";
                System.out.format(FormatString,
                                  Utils.paddedString("REPLICA#", COLUMN_WIDTH),
                                  Utils.paddedString("PARTITION", COLUMN_WIDTH),
                                  Utils.paddedString("NODE", COLUMN_WIDTH));
                for(int i = 0; i < numReplicas; i++) {
                    Integer nodeId = bRoutingPlan.getNodeIdForZoneNary(zone.getId(), i, key);
                    Integer partitionId = routingPlan.getNodesPartitionIdForKey(nodeId, key);
                    System.out.format(FormatString,
                                      Utils.paddedString(i + "", COLUMN_WIDTH),
                                      Utils.paddedString(partitionId.toString(), COLUMN_WIDTH),
                                      Utils.paddedString(nodeId + "("
                                                         + cluster.getNodeById(nodeId).getHost()
                                                         + ")", COLUMN_WIDTH));
                }
                System.out.println();
            }

            System.out.println("-----------------------------------------------");
            System.out.println();
        }
    }

    private static boolean isAvroSchema(String serializerName) {
        if(serializerName.equals(DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME)
           || serializerName.equals(DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME)
           || serializerName.equals(DefaultSerializerFactory.AVRO_REFLECTIVE_TYPE_NAME)
           || serializerName.equals(DefaultSerializerFactory.AVRO_SPECIFIC_TYPE_NAME)) {
            return true;
        } else {
            return false;
        }
    }

    private static boolean confirmMetadataUpdate(Integer nodeId,
                                                 AdminClient adminClient,
                                                 Object value) {
        List<Integer> nodeIds = Lists.newArrayList();

        System.out.print("\nNew metadata: \n" + value.toString() + "\n");
        System.out.print("\nAffected nodes:\n");
        System.out.format("+-------+------+---------------------------------+----------+---------+------------------+%n");
        System.out.printf("|Id     |Zone  |Host                             |SocketPort|AdminPort|NumberOfPartitions|%n");
        System.out.format("+-------+------+---------------------------------+----------+---------+------------------+%n");

        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
                System.out.format("| %-5d | %-4d | %-31s | %-5d    | %-5d   | %-5d            |%n",
                                  node.getId(),
                                  node.getZoneId(),
                                  node.getHost(),
                                  node.getSocketPort(),
                                  node.getAdminPort(),
                                  node.getNumberOfPartitions());
                System.out.format("+-------+------+---------------------------------+----------+---------+------------------+%n");
            }
        }

        System.out.print("Do you want to proceed? [Y/N]: ");
        Scanner in = new Scanner(System.in);
        String choice = in.nextLine();

        if(choice.equals("Y") || choice.equals("y")) {
            return true;
        } else if(choice.equals("N") || choice.equals("n")) {
            return false;
        } else {
            System.out.println("Incorrect response detected. Exiting.");
            return false;
        }
    }
}

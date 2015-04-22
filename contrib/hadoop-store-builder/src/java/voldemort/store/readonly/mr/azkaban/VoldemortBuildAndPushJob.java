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

package voldemort.store.readonly.mr.azkaban;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import azkaban.jobExecutor.AbstractJob;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.disk.KeyValueWriter;
import voldemort.store.readonly.hooks.BuildAndPushHook;
import voldemort.store.readonly.hooks.BuildAndPushStatus;
import voldemort.store.readonly.mr.azkaban.VoldemortStoreBuilderJob.VoldemortStoreBuilderConf;
import voldemort.store.readonly.mr.azkaban.VoldemortSwapJob.VoldemortSwapConf;
import voldemort.store.readonly.mr.utils.AvroUtils;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.JsonSchema;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.DeleteAllFailedFetchStrategy;
import voldemort.store.readonly.swapper.DisableStoreOnFailedNodeFailedFetchStrategy;
import voldemort.store.readonly.swapper.FailedFetchLock;
import voldemort.store.readonly.swapper.FailedFetchStrategy;
import voldemort.store.readonly.swapper.HdfsFailedFetchLock;
import voldemort.store.readonly.swapper.RecoverableFailedFetchException;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class VoldemortBuildAndPushJob extends AbstractJob {

    private final Logger log;

    // CONFIG VALUES (and other internal state)

    private final Props props;
    private Cluster cluster;
    private List<StoreDefinition> storeDefs;
    private final String storeName;
    private final List<String> clusterURLs;
    private final Map<String, AdminClient> adminClientPerCluster;
    private final int nodeId;
    private final List<String> dataDirs;
    private final boolean isAvroJob;
    private final String keyFieldName;
    private final String valueFieldName;
    private final boolean isAvroVersioned;
    private final long minNumberOfRecords;
    private final String hdfsFetcherPort;
    private final String hdfsFetcherProtocol;
    private String jsonKeyField;
    private String jsonValueField;
    private String reducerOutputCompressionCodec;
    private final Set<BuildAndPushHook> hooks = new HashSet<BuildAndPushHook>();
    private final int heartBeatHookIntervalTime;
    private final HeartBeatHookRunnable heartBeatHookRunnable;
    private final boolean pushHighAvailability;
    private final List<String> pushHighAvailabilityClusterWhiteList;

    // CONFIG NAME CONSTANTS

    // build.required
    public final static String BUILD_INPUT_PATH = "build.input.path";
    public final static String BUILD_OUTPUT_DIR = "build.output.dir";
    // build.optional
    public final static String BUILD_TEMP_DIR = "build.temp.dir";
    public final static String BUILD_REPLICATION_FACTOR = "build.replication.factor";
    public final static String BUILD_COMPRESS_VALUE = "build.compress.value";
    public final static String BUILD_CHUNK_SIZE = "build.chunk.size";
    public final static String BUILD_OUTPUT_KEEP = "build.output.keep";
    public final static String BUILD_TYPE_AVRO = "build.type.avro";
    public final static String BUILD_REQUIRED_READS = "build.required.reads";
    public final static String BUILD_REQUIRED_WRITES = "build.required.writes";
    public final static String BUILD_FORCE_SCHEMA_KEY = "build.force.schema.key";
    public final static String BUILD_FORCE_SCHEMA_VALUE = "build.force.schema.value";
    public final static String BUILD_PREFERRED_READS = "build.preferred.reads";
    public final static String BUILD_PREFERRED_WRITES = "build.preferred.writes";
    // push.required
    public final static String PUSH_STORE_NAME = "push.store.name";
    public final static String PUSH_CLUSTER = "push.cluster";
    public final static String PUSH_STORE_OWNERS = "push.store.owners";
    public final static String PUSH_STORE_DESCRIPTION = "push.store.description";
    // push.optional
    public final static String PUSH_HTTP_TIMEOUT_SECONDS = "push.http.timeout.seconds";
    public final static String PUSH_NODE = "push.node";
    public final static String PUSH_VERSION = "push.version";
    public final static String PUSH_VERSION_TIMESTAMP = "push.version.timestamp";
    public final static String PUSH_BACKOFF_DELAY_SECONDS = "push.backoff.delay.seconds";
    public final static String PUSH_ROLLBACK = "push.rollback";
    public final static String PUSH_FORCE_SCHEMA_KEY = "push.force.schema.key";
    public final static String PUSH_FORCE_SCHEMA_VALUE = "push.force.schema.value";
    public final static String PUSH_HA_ENABLED = "push.ha.enabled";
    public final static String PUSH_HA_CLUSTER_WHITELIST = "push.ha.cluster.whitelist";
    public final static String PUSH_HA_MAX_NODE_FAILURE = "push.ha.max.node.failure";
    public final static String PUSH_HA_LOCK_IMPLEMENTATION = "push.ha.lock.implementation";
    // others.optional
    public final static String KEY_SELECTION = "key.selection";
    public final static String VALUE_SELECTION = "value.selection";
    public final static String NUM_CHUNKS = "num.chunks";
    public final static String BUILD = "build";
    public final static String PUSH = "push";
    public final static String VOLDEMORT_FETCHER_PROTOCOL = "voldemort.fetcher.protocol";
    public final static String VOLDEMORT_FETCHER_PORT = "voldemort.fetcher.port";
    public final static String AVRO_SERIALIZER_VERSIONED = "avro.serializer.versioned";
    public final static String AVRO_KEY_FIELD = "avro.key.field";
    public final static String AVRO_VALUE_FIELD = "avro.value.field";
    public final static String HADOOP_JOB_UGI = "hadoop.job.ugi";
    public final static String REDUCER_PER_BUCKET = "reducer.per.bucket";
    public final static String CHECKSUM_TYPE = "checksum.type";
    public final static String SAVE_KEYS = "save.keys";
    public final static String HEARTBEAT_HOOK_INTERVAL_MS = "heartbeat.hook.interval.ms";
    public final static String HOOKS = "hooks";
    public final static String MIN_NUMBER_OF_RECORDS = "min.number.of.records";
    public final static String REDUCER_OUTPUT_COMPRESS_CODEC = "reducer.output.compress.codec";
    public static final String REDUCER_OUTPUT_COMPRESS = "reducer.output.compress";

    // provided
    public final static String AZKABAN_LINK_EXECUTION_URL = "azkaban.link.execution.url";

    public VoldemortBuildAndPushJob(String name, azkaban.utils.Props azkabanProps) {
        super(name, Logger.getLogger(name));
        this.log = getLog();
        log.info("Job props.toString(): " + azkabanProps.toString());

        this.props = new Props(azkabanProps.toProperties());
        this.storeName = props.getString(PUSH_STORE_NAME).trim();
        this.clusterURLs = new ArrayList<String>();
        this.dataDirs = new ArrayList<String>();
        this.adminClientPerCluster = Maps.newHashMap();

        String clusterUrlText = props.getString(PUSH_CLUSTER);
        for(String url: Utils.COMMA_SEP.split(clusterUrlText.trim())) {
            if(url.trim().length() > 0) {
                this.clusterURLs.add(url);
                this.adminClientPerCluster.put(url,
                        new AdminClient(url, new AdminClientConfig(), new ClientConfig()));
            }
        }

        if(clusterURLs.size() <= 0)
            throw new RuntimeException("Number of urls should be atleast 1");

        // Support multiple output dirs if the user mentions only PUSH, no
        // BUILD.
        // If user mentions both then should have only one
        String dataDirText = props.getString(BUILD_OUTPUT_DIR);
        for(String dataDir: Utils.COMMA_SEP.split(dataDirText.trim()))
            if(dataDir.trim().length() > 0)
                this.dataDirs.add(dataDir);

        if(dataDirs.size() <= 0)
            throw new RuntimeException("Number of data dirs should be atleast 1");

        this.nodeId = props.getInt(PUSH_NODE, 0);

        this.hdfsFetcherProtocol = props.getString(VOLDEMORT_FETCHER_PROTOCOL, "hftp");
        this.hdfsFetcherPort = props.getString(VOLDEMORT_FETCHER_PORT, "50070");

        log.info(VOLDEMORT_FETCHER_PROTOCOL + " is set to : " + hdfsFetcherProtocol);
        log.info(VOLDEMORT_FETCHER_PORT + " is set to : " + hdfsFetcherPort);

        isAvroJob = props.getBoolean(BUILD_TYPE_AVRO, false);

        // Set default to false
        // this ensures existing clients who are not aware of the new serializer
        // type dont bail out
        isAvroVersioned = props.getBoolean(AVRO_SERIALIZER_VERSIONED, false);

        keyFieldName = props.getString(AVRO_KEY_FIELD, null);

        valueFieldName = props.getString(AVRO_VALUE_FIELD, null);

        if(isAvroJob) {
            if(keyFieldName == null)
                throw new RuntimeException("The key field must be specified in the properties for the Avro build and push job!");
            if(valueFieldName == null)
                throw new RuntimeException("The value field must be specified in the properties for the Avro build and push job!");
        }

        minNumberOfRecords = props.getLong(MIN_NUMBER_OF_RECORDS, 1);

        pushHighAvailabilityClusterWhiteList = props.getList(PUSH_HA_CLUSTER_WHITELIST, new ArrayList<String>());
        pushHighAvailability = props.getBoolean(PUSH_HA_ENABLED, false);
        if (pushHighAvailability) {
            for (String clusterUrl: clusterURLs) {
                if (!pushHighAvailabilityClusterWhiteList.contains(clusterUrl)) {
                    log.warn("This cluster is not whitelisted for PUSH HA: " + pushHighAvailability);
                }
            }
        }

        // Initializing hooks
        heartBeatHookIntervalTime = props.getInt(HEARTBEAT_HOOK_INTERVAL_MS, 60000);
        heartBeatHookRunnable = new HeartBeatHookRunnable(heartBeatHookIntervalTime);
        String hookNamesText = props.getString(HOOKS, null);
        if (hookNamesText != null && !hookNamesText.isEmpty()) {
            for (String hookName : Utils.COMMA_SEP.split(hookNamesText.trim())) {
                try {
                    BuildAndPushHook hook = (BuildAndPushHook) ReflectUtils.callConstructor(Class.forName(hookName));
                    try {
                        hook.init(props);
                        log.info("Initialized BuildAndPushHook [" + hook.getName() + "]");
                        hooks.add(hook);
                    } catch (Exception e) {
                        log.warn("Failed to initialize BuildAndPushHook [" + hook.getName() + "]. It will not be invoked.", e);
                    }
                } catch (ClassNotFoundException e) {
                    log.error("The requested BuildAndPushHook [" + hookName + "] was not found! Check your classpath and config!", e);
                }
            }
        }
    }

    private void invokeHooks(BuildAndPushStatus status) {
        invokeHooks(status, null);
    }

    private void invokeHooks(BuildAndPushStatus status, String details) {
        for (BuildAndPushHook hook : hooks) {
            try {
                hook.invoke(status, details);
            } catch (Exception e) {
                // Hooks are never allowed to fail a job...
                log.warn("Failed to invoke BuildAndPushHook [" + hook.getName() + "] because of exception: ", e);
            }
        }
    }

    /**
     * 
     * Compare two clusters to see if they have the equal number of partitions,
     * equal number of nodes and each node hosts the same partition ids.
     * 
     * @param lhs Left hand side Cluster object
     * @param rhs Right hand side cluster object
     * @return True if the clusters are congruent (equal number of partitions,
     *         equal number of nodes and same partition ids
     */
    private boolean areTwoClustersEqual(final Cluster lhs, final Cluster rhs) {
        if (lhs.getNumberOfPartitions() != rhs.getNumberOfPartitions())
            return false;
        if (!lhs.getNodeIds().equals(rhs.getNodeIds()))
            return false;
        for (Node lhsNode: lhs.getNodes()) {
            Node rhsNode = rhs.getNodeById(lhsNode.getId());
            if (!rhsNode.getPartitionIds().equals(lhsNode.getPartitionIds())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if all cluster objects in the list are congruent.
     * 
     * @param clusterUrls of cluster objects
     * @return
     * 
     */
    private void allClustersEqual(final List<String> clusterUrls) {
        Validate.notEmpty(clusterUrls, "Clusterurls cannot be null");
        // If only one clusterUrl return immediately
        if (clusterUrls.size() == 1)
            return;
        AdminClient adminClientLhs = adminClientPerCluster.get(clusterUrls.get(0));
        Cluster clusterLhs = adminClientLhs.getAdminClientCluster();
        for (int index = 1; index < clusterUrls.size(); index++) {
            AdminClient adminClientRhs = adminClientPerCluster.get(clusterUrls.get(index));
            Cluster clusterRhs = adminClientRhs.getAdminClientCluster();
            if (!areTwoClustersEqual(clusterLhs, clusterRhs))
                throw new VoldemortException("Cluster " + clusterLhs.getName()
                                             + "is not the same as " + clusterRhs.getName());
        }
    }
    
    private void checkForPreconditions(boolean build, boolean push) {
        if (!build && !push) {
            throw new RuntimeException(" Both build and push cannot be false ");
        }
        else if (build && push && dataDirs.size() != 1) {
            // Should have only one data directory (which acts like the parent directory to all urls)
            throw new RuntimeException(" Should have only one data directory ( which acts like root "
                                       + " directory ) since they are auto-generated during build phase ");
        } else if (!build && push && dataDirs.size() != clusterURLs.size()) {
            // Since we are only pushing number of data directories should be equal to number of cluster urls
            throw new RuntimeException(" Since we are only pushing, number of data directories"
                                       + " ( comma separated ) should be equal to number of cluster"
                                       + " urls ");
        }
        if ((!build && push) || (build && !push)) {
            log.warn("DEPRECATED : Creating one build job and separate push job is a deprecated strategy. Instead create"
                     + "just one job with both build and push set as true and pass a list of cluster urls.");
        }
    }

    private String getMatchingServerSupportedCompressionCodec(int nodeId) {
        /*
         * Strict operational assumption made by this method:
         * 
         * All servers have symmetrical settings.
         * 
         * TODO Currently this method requests only one server in one of the
         * clusters to check for Server supported compression codec. This could
         * be a problem if we were to do rolling upgrade on RO servers AND still
         * allow for Bnp jobs to progress.
         * 
         * Fix: The ideal solution is to check all nodes in all colos to ensure
         * all of them support same configs for compression.
         * 
         * Currently this is okay since we anyway dont do rolling bounce and
         * stop all Bnp jobs for any kind of maintenance.
         */

        AdminClient adminclient = new AdminClient(clusterURLs.get(0),
                                                  new AdminClientConfig(),
                                                  new ClientConfig());
        log.info("Requesting Compression Codec expected by Server");
        
        List<String> supportedCodecs;
        try{
            supportedCodecs = adminclient.readonlyOps.getSupportedROStorageCompressionCodecs(nodeId);
        } catch(Exception e) {
            log.error("Exception thrown when requesting for supported compression codecs. Server might be running in a older version. Exception: "
                     + e.getMessage());
            // return here
            return null;
        }
        
        String codecList = "[ ";
        for(String str: supportedCodecs) {
            codecList += str + " ";
        }
        codecList += "]";
        log.info("Server responded with compression codecs: " + codecList);
        /*
         * TODO for now only checking if there is a match between the server
         * supported codec and the one that we support. Later this method can be
         * extended to add more compression types or pick the first type
         * returned by the server.
         */

        for(String codecStr: supportedCodecs) {
            if(codecStr.toUpperCase(Locale.ENGLISH).equals(KeyValueWriter.COMPRESSION_CODEC)) {
                return codecStr;
            }
        }
        return null; // no matching compression codec. defaults to uncompressed
                     // data.
    }

    @Override
    public void run() throws Exception {
        invokeHooks(BuildAndPushStatus.STARTING);
        if (hooks.size() > 0) {
            Thread t = new Thread(heartBeatHookRunnable);
            t.setDaemon(true);
            t.start();
        }

        try {
            // These two options control the build and push phases of the job respectively.
            boolean build = props.getBoolean(BUILD, true);
            boolean push = props.getBoolean(PUSH, true);
            jsonKeyField = props.getString(KEY_SELECTION, null);
            jsonValueField = props.getString(VALUE_SELECTION, null);

            checkForPreconditions(build, push);

            try {
                allClustersEqual(clusterURLs);
            } catch(VoldemortException e) {
                log.error("Exception during cluster equality check", e);
                fail("Exception during cluster equality check: " + e.toString());
                return;
            }

            reducerOutputCompressionCodec = getMatchingServerSupportedCompressionCodec(nodeId);
            if(reducerOutputCompressionCodec != null) {
                log.info("Using compression codec: " + reducerOutputCompressionCodec);
                props.put(REDUCER_OUTPUT_COMPRESS, "true");
                props.put(REDUCER_OUTPUT_COMPRESS_CODEC, reducerOutputCompressionCodec);
            } else {
                log.info("Using no compression");
            }

            // Create a hashmap to capture exception per url
            HashMap<String, Exception> exceptions = Maps.newHashMap();
            String buildOutputDir = null;
            for (int index = 0; index < clusterURLs.size(); index++) {
                String url = clusterURLs.get(index);
                if (isAvroJob) {
                    // Verify the schema if the store exists or else add the new store
                    verifyOrAddStoreAvro(url, isAvroVersioned);
                } else {
                    // Verify the schema if the store exists or else add the new store
                    verifyOrAddStore(url);
                }
                if (build) {
                    // If we are only building and not pushing then we want the build to
                    // happen on all three clusters || we are pushing and we want to build
                    // it to only once
                    if (!push || buildOutputDir == null) {
                        try {
                            invokeHooks(BuildAndPushStatus.BUILDING);
                            buildOutputDir = runBuildStore(props, url);
                        } catch(Exception e) {
                            log.error("Exception during build for url " + url, e);
                            exceptions.put(url, e);
                        }
                    }
                }
                if (push) {
                    if (log.isDebugEnabled()) {
                        log.debug("Informing about push start ...");
                    }
                    log.info("Pushing to cluster url " + clusterURLs.get(index));
                    // If we are not building and just pushing then we want to get the built
                    // from the dataDirs, or else we will just the one that we built earlier
                    try {
                        if (!build) {
                            buildOutputDir = dataDirs.get(index);
                        }
                        // If there was an exception during the build part the buildOutputDir might be null, check
                        // if that's the case, if yes then continue and don't even try pushing
                        if (buildOutputDir == null) {
                            continue;
                        }
                        invokeHooks(BuildAndPushStatus.PUSHING, url);
                        runPushStore(props, url, buildOutputDir);
                        invokeHooks(BuildAndPushStatus.SWAPPED, url);
                    } catch (RecoverableFailedFetchException e) {
                        log.warn("There was a problem with some of the fetches, but a swap was still able to go through!", e);
                        invokeHooks(BuildAndPushStatus.SWAPPED_WITH_FAILURES, url);
                    } catch(Exception e) {
                        log.error("Exception during push for url " + url, e);
                        exceptions.put(url, e);
                    }
                }
            }
            if(build && push && buildOutputDir != null
               && !props.getBoolean(BUILD_OUTPUT_KEEP, false)) {
                JobConf jobConf = new JobConf();
                if(props.containsKey(HADOOP_JOB_UGI)) {
                    jobConf.set(HADOOP_JOB_UGI, props.getString(HADOOP_JOB_UGI));
                }
                log.info("Informing about delete start ..." + buildOutputDir);
                HadoopUtils.deletePathIfExists(jobConf, buildOutputDir);
                log.info("Deleted " + buildOutputDir);
            }

            if (exceptions.size() == 0) {
                invokeHooks(BuildAndPushStatus.FINISHED);
                cleanUp();
            } else {
                log.error("Got exceptions during Build and Push:");
                for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                    log.error("Exception for cluster: " + entry.getKey(), entry.getValue());
                }
                throw new VoldemortException("Got exceptions during Build and Push");
            }
        } catch (Exception e) {
            log.error("An exception occurred during Build and Push !!", e);
            fail(e.toString());
            throw e;
        } catch (Throwable t) {
            // This is for OOMs, StackOverflows and other uber nasties...
            // We'll try to invoke hooks but all bets are off at this point :/
            log.fatal("A non-Exception Throwable was caught! (OMG) We will try to invoke hooks on a best effort basis...", t);
            fail(t.toString());
            // N.B.: Azkaban's AbstractJob#run throws Exception, not Throwable, so we can't rethrow directly...
            throw new Exception("A non-Exception Throwable was caught! Bubbling it up as an Exception...", t);
        }
    }

    @Override
    public void cancel() throws java.lang.Exception {
        log.info("VoldemortBuildAndPushJob.cancel() has been called!");
        invokeHooks(BuildAndPushStatus.CANCELLED);
        cleanUp();
    }

    private void fail(String details) {
        invokeHooks(BuildAndPushStatus.FAILED, details);
        cleanUp();
    }

    private void cleanUp() {
        heartBeatHookRunnable.stop();
        for (AdminClient adminClient: adminClientPerCluster.values()) {
            adminClient.close();
        }
    }

    /**
     * Checks whether the store is already present in the cluster and verify its schema, otherwise add it
     * 
     * @param url to check
     * @return
     * 
     */
    private void verifyOrAddStore(String url) throws Exception {
        // create new json store def with schema from the metadata in the input path
        JsonSchema schema = HadoopUtils.getSchemaFromPath(getInputPath());
        int replicationFactor = props.getInt(BUILD_REPLICATION_FACTOR, 2);
        int requiredReads = props.getInt(BUILD_REQUIRED_READS, 1);
        int requiredWrites = props.getInt(BUILD_REQUIRED_WRITES, 1);
        String description = props.getString(PUSH_STORE_DESCRIPTION, "");
        String owners = props.getString(PUSH_STORE_OWNERS, "");
        String keySchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getKeyType() + "</schema-info>\n\t";

        if(jsonKeyField != null && jsonKeyField.length() > 0) {
            keySchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                        + schema.getKeyType().subtype(jsonKeyField) + "</schema-info>\n\t";
        }

        String valSchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getValueType() + "</schema-info>\n\t";

        if (jsonValueField != null && jsonValueField.length() > 0) {
            valSchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                        + schema.getValueType().subtype(jsonValueField) + "</schema-info>\n\t";
        }

        boolean hasCompression = false;
        if(props.containsKey(BUILD_COMPRESS_VALUE)) {
            hasCompression = true;
        }

        if(hasCompression) {
            valSchema += "\t<compression><type>gzip</type></compression>\n\t";
        }

        if(props.containsKey(BUILD_FORCE_SCHEMA_KEY)) {
            keySchema = props.get(BUILD_FORCE_SCHEMA_KEY);
        }

        if(props.containsKey(BUILD_FORCE_SCHEMA_VALUE)) {
            valSchema = props.get(BUILD_FORCE_SCHEMA_VALUE);
        }

        String newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                              replicationFactor,
                                                              requiredReads,
                                                              requiredWrites,
                                                              props.containsKey(BUILD_PREFERRED_READS) ? props.getInt(BUILD_PREFERRED_READS)
                                                                                                        : null,
                                                              props.containsKey(BUILD_PREFERRED_WRITES) ? props.getInt(BUILD_PREFERRED_WRITES)
                                                                                                         : null,
                                                              (props.containsKey(PUSH_FORCE_SCHEMA_KEY)) ? props.getString(PUSH_FORCE_SCHEMA_KEY)
                                                                                                          : keySchema,
                                                              (props.containsKey(PUSH_FORCE_SCHEMA_VALUE)) ? props.getString(PUSH_FORCE_SCHEMA_VALUE)
                                                                                                            : valSchema,
                                                              description,
                                                              owners);
        boolean foundStore = findAndVerify(url,
                                           newStoreDefXml,
                                           hasCompression,
                                           replicationFactor,
                                           requiredReads,
                                           requiredWrites);
        if (!foundStore) {
            try {
                StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);
                addStore(description, owners, url, newStoreDef);
            }
            catch(RuntimeException e) {
                log.error("Getting store definition from: " + url + " (node id " + this.nodeId + ")", e);
                fail("Failed to add store");
                throw new VoldemortException("Failed to add store", e);
            }
        }
        // don't use newStoreDef because we want to ALWAYS use the JSON definition since the store
        // builder assumes that you are using JsonTypeSerializer. This allows you to tweak your 
        // value/key store xml  as you see fit, but still uses the json sequence file meta data
        // to  build the store.
        storeDefs = ImmutableList.of(VoldemortUtils.getStoreDef(VoldemortUtils.getStoreDefXml(storeName,
                                                                                              replicationFactor,
                                                                                              requiredReads,
                                                                                              requiredWrites,
                                                                                              props.containsKey(BUILD_PREFERRED_READS) ? props.getInt(BUILD_PREFERRED_READS)
                                                                                                                                        : null,
                                                                                              props.containsKey(BUILD_PREFERRED_WRITES) ? props.getInt(BUILD_PREFERRED_WRITES)
                                                                                                                                         : null,
                                                                                              keySchema,
                                                                                              valSchema)));
        cluster = adminClientPerCluster.get(url).getAdminClientCluster();
    }
    
    /**
     * Check if store exists and then verify the schema. Returns false if store doesn't exist
     * 
     * @param url to check
     * @param newStoreDefXml
     * @param hasCompression
     * @param replicationFactor
     * @param requiredReads
     * @param requiredWrites
     * @return boolean value true means store exists, false otherwise
     * 
     */

    private boolean findAndVerify(String url,
                                  String newStoreDefXml,
                                  boolean hasCompression,
                                  int replicationFactor,
                                  int requiredReads,
                                  int requiredWrites) {
        log.info("Verifying store: \n" + newStoreDefXml.toString());
        StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);
        log.info("Getting store definition from: " + url + " (node id " + this.nodeId + ")");
        List<StoreDefinition> remoteStoreDefs = adminClientPerCluster.get(url).metadataMgmtOps.getRemoteStoreDefList(this.nodeId)
                                                                           .getValue();
        boolean foundStore = false;
        // go over all store defs and see if one has the same name as the store we're trying to build
        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(storeName)) {
                // if the store already exists, but doesn't match what we want to push, we need to worry
                if(!remoteStoreDef.equals(newStoreDef)) {
                    // it is possible that the stores actually DO match, but the json in the key/value
                    // serializers is out of order (eg {'a': 'int32', 'b': 'int32'}  could have a/b reversed.
                    // This is just a reflection of the fact that voldemort json type defs use hashmaps that
                    // are unordered, and pig uses bags that are unordered  as well. it's therefore unpredictable
                    // what order the keys will come out of pig. let's check to see if the key/value
                    // serializers are REALLY equal.
                    SerializerDefinition localKeySerializerDef = newStoreDef.getKeySerializer();
                    SerializerDefinition localValueSerializerDef = newStoreDef.getValueSerializer();
                    SerializerDefinition remoteKeySerializerDef = remoteStoreDef.getKeySerializer();
                    SerializerDefinition remoteValueSerializerDef = remoteStoreDef.getValueSerializer();

                    if(remoteKeySerializerDef.getName().equals("json")
                       && remoteValueSerializerDef.getName().equals("json")
                       && remoteKeySerializerDef.getAllSchemaInfoVersions().size() == 1
                       && remoteValueSerializerDef.getAllSchemaInfoVersions().size() == 1) {
                        JsonTypeDefinition remoteKeyDef = JsonTypeDefinition.fromJson(remoteKeySerializerDef.getCurrentSchemaInfo());
                        JsonTypeDefinition remoteValDef = JsonTypeDefinition.fromJson(remoteValueSerializerDef.getCurrentSchemaInfo());
                        JsonTypeDefinition localKeyDef = JsonTypeDefinition.fromJson(localKeySerializerDef.getCurrentSchemaInfo());
                        JsonTypeDefinition localValDef = JsonTypeDefinition.fromJson(localValueSerializerDef.getCurrentSchemaInfo());

                        if(remoteKeyDef.equals(localKeyDef) && remoteValDef.equals(localValDef)) {
                            String compressionPolicy = "";
                            if(hasCompression) {
                                compressionPolicy = "\n\t\t<compression><type>gzip</type></compression>";
                            }
                            // if the key/value serializers are REALLY equal (even though the strings may not match), then
                            // just use the remote stores to GUARANTEE that they match, and try again.
                            newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                                           replicationFactor,
                                                                           requiredReads,
                                                                           requiredWrites,
                                                                           props.containsKey(BUILD_PREFERRED_READS) ? props.getInt(BUILD_PREFERRED_READS)
                                                                                                                     : null,
                                                                           props.containsKey(BUILD_PREFERRED_WRITES) ? props.getInt(BUILD_PREFERRED_WRITES)
                                                                                                                      : null,
                                                                           "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                                                                                   + remoteKeySerializerDef.getCurrentSchemaInfo()
                                                                                   + "</schema-info>\n\t",
                                                                           "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                                                                                   + remoteValueSerializerDef.getCurrentSchemaInfo()
                                                                                   + "</schema-info>"
                                                                                   + compressionPolicy
                                                                                   + "\n\t");

                            newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);
                            if(!remoteStoreDef.equals(newStoreDef)) {
                                // if we still get a fail, then we know that the store defs don't match for reasons
                                // OTHER than the key/value serializer
                                throw new RuntimeException("Your store schema is identical, but the store definition does not match. Have: "
                                                           + newStoreDef + "\nBut expected: " + remoteStoreDef);
                            }
                        } else {
                            // if the key/value serializers are not equal (even in java, not just json strings),
                            // then fail
                            throw new RuntimeException("Your store definition does not match the store definition that is already in the cluster. Tried to resolve identical schemas between local and remote, but failed. Have: "
                                                       + newStoreDef + "\nBut expected: " + remoteStoreDef);
                        }
                    } else {
                        throw new RuntimeException("Your store definition does not match the store definition that is already in the cluster. Have: "
                                                   + newStoreDef + "\nBut expected: " + remoteStoreDef);
                    }
                }
                foundStore = true;
                break;
            }
        }
        return foundStore;
    }
    
    private void addStore(String description, String owners, String url, StoreDefinition newStoreDef) {
        if (description.length() == 0) {
            throw new RuntimeException("Description field missing in store definition. "
                                       + "Please add \"" + PUSH_STORE_DESCRIPTION
                                       + "\" with a line describing your store");
        }
        if (owners.length() == 0) {
            throw new RuntimeException("Owner field missing in store definition. "
                                       + "Please add \""
                                       + PUSH_STORE_OWNERS
                                       + "\" with value being comma-separated list of LinkedIn email ids");

        }
        log.info("Could not find store " + storeName + " on Voldemort. Adding it to all nodes ");
        try {
            adminClientPerCluster.get(url).storeMgmtOps.addStore(newStoreDef);
        }
        catch(VoldemortException ve) {
            throw new RuntimeException("Exception during adding store" + ve.getMessage());
        }
    }

    public String runBuildStore(Props props, String url) throws Exception {
        int replicationFactor = props.getInt(BUILD_REPLICATION_FACTOR, 2);
        int chunkSize = props.getInt(BUILD_CHUNK_SIZE, 1024 * 1024 * 1024);
        Path tempDir = new Path(props.getString(BUILD_TEMP_DIR, "/tmp/vold-build-and-push-"
                + new Random().nextLong()));
        URI uri = new URI(url);
        Path outputDir = new Path(props.getString(BUILD_OUTPUT_DIR), uri.getHost());
        Path inputPath = getInputPath();
        CheckSumType checkSumType = CheckSum.fromString(props.getString(CHECKSUM_TYPE,
                                                                        CheckSum.toString(CheckSumType.MD5)));
        boolean saveKeys = props.getBoolean(SAVE_KEYS, true);
        boolean reducerPerBucket = props.getBoolean(REDUCER_PER_BUCKET, false);
        int numChunks = props.getInt(NUM_CHUNKS, -1);

        String recSchema = null;
        String keySchema = null;
        String valSchema = null;

        if(isAvroJob) {
            recSchema = getRecordSchema();
            keySchema = getKeySchema();
            valSchema = getValueSchema();
        }
        new VoldemortStoreBuilderJob(
                this.getId() + "-build-store",
                props,
                new VoldemortStoreBuilderConf(
                        replicationFactor,
                        chunkSize,
                        tempDir,
                        outputDir,
                        inputPath,
                        cluster,
                        storeDefs,
                        storeName,
                        checkSumType,
                        saveKeys,
                        reducerPerBucket,
                        numChunks,
                        keyFieldName,
                        valueFieldName,
                        recSchema,
                        keySchema,
                        valSchema,
                        isAvroJob,
                        minNumberOfRecords)).run();
        return outputDir.toString();
    }

    public void runPushStore(Props props, String url, String dataDir) throws Exception {
        // For backwards compatibility http timeout = admin timeout
        int httpTimeoutMs = 1000 * props.getInt(PUSH_HTTP_TIMEOUT_SECONDS, 24 * 60 * 60);
        long pushVersion = props.getLong(PUSH_VERSION, -1L);
        if(props.containsKey(PUSH_VERSION_TIMESTAMP)) {
            DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            pushVersion = Long.parseLong(format.format(new Date()));
        }
        int maxBackoffDelayMs = 1000 * props.getInt(PUSH_BACKOFF_DELAY_SECONDS, 60);

        List<FailedFetchStrategy> failedFetchStrategyList = Lists.newArrayList();

        boolean rollback = props.getBoolean(PUSH_ROLLBACK, true);
        boolean disableStoreOnFailedNodeForThisCluster =
                pushHighAvailability && pushHighAvailabilityClusterWhiteList.contains(url);

        if (disableStoreOnFailedNodeForThisCluster) {
            // TODO: This could instead interrogate the cluster
            // to find out its minimum replication factor across all stores...
            int maxNodeFailure = props.getInt(PUSH_HA_MAX_NODE_FAILURE, 1);
            Class<? extends FailedFetchLock> failedFetchLockClass = (Class<? extends FailedFetchLock>)
                    props.getClass(PUSH_HA_LOCK_IMPLEMENTATION, HdfsFailedFetchLock.class);
            String processId = props.getString(AZKABAN_LINK_EXECUTION_URL);
            Object[] failedFetchLockParameters = new Object[]{props, url, processId};
            FailedFetchLock failedFetchLock = ReflectUtils.callConstructor(failedFetchLockClass, failedFetchLockParameters);
            failedFetchStrategyList.add(
                    new DisableStoreOnFailedNodeFailedFetchStrategy(
                            adminClientPerCluster.get(url), failedFetchLock, maxNodeFailure));
            log.info("Beginning push with pushHighAvailability enabled for cluster: " + url);
        } else if (pushHighAvailability && !disableStoreOnFailedNodeForThisCluster) {
            log.warn("Beginning push with pushHighAvailability DISABLED because the cluster is not whitelisted: " + url);
        }

        if (rollback) {
            failedFetchStrategyList.add(
                    new DeleteAllFailedFetchStrategy(adminClientPerCluster.get(url)));
        }

        new VoldemortSwapJob(
                this.getId() + "-push-store",
                props,
                new VoldemortSwapConf(
                        cluster,
                        dataDir,
                        storeName,
                        httpTimeoutMs,
                        pushVersion,
                        maxBackoffDelayMs,
                        rollback,
                        failedFetchStrategyList)).run();
    }

    /**
     * Get the sanitized input path. At the moment of writing, this means the
     * #LATEST tag is expanded.
     */
    private Path getInputPath() throws IOException {
        Path path = new Path(props.getString(BUILD_INPUT_PATH));
        return HadoopUtils.getSanitizedPath(path);
    }

    // Get the schema for the Avro Record from the object container file
    public String getRecordSchema() throws IOException {
        Schema schema = AvroUtils.getAvroSchemaFromPath(getInputPath());
        String recSchema = schema.toString();
        return recSchema;
    }

    // Extract schema of the key field
    public String getKeySchema() throws IOException {
        Schema schema = AvroUtils.getAvroSchemaFromPath(getInputPath());
        String keySchema = schema.getField(keyFieldName).schema().toString();
        return keySchema;
    }

    // Extract schema of the value field
    public String getValueSchema() throws IOException {
        Schema schema = AvroUtils.getAvroSchemaFromPath(getInputPath());
        String valueSchema = schema.getField(valueFieldName).schema().toString();
        return valueSchema;
    }
    
    private static class KeyValueSchema {
        String keySchema;
        String valSchema;
        KeyValueSchema(String key, String val) {
            keySchema = key;
            valSchema = val;
        }
    }
    
    // Verify if the new avro schema being pushed is the same one as the last version present on the server
    // supports schema evolution

    public void verifyOrAddStoreAvro(String url, boolean isVersioned) throws Exception {
        // create new n store def with schema from the metadata in the input path
        Schema schema = AvroUtils.getAvroSchemaFromPath(getInputPath());
        int replicationFactor = props.getInt(BUILD_REPLICATION_FACTOR, 2);
        int requiredReads = props.getInt(BUILD_REQUIRED_READS, 1);
        int requiredWrites = props.getInt(BUILD_REQUIRED_WRITES, 1);
        String description = props.getString(PUSH_STORE_DESCRIPTION, "");
        String owners = props.getString(PUSH_STORE_OWNERS, "");
        String serializerName;
        if (isVersioned)
            serializerName = DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME;
        else
            serializerName = DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME;

        boolean hasCompression = false;
        if(props.containsKey(BUILD_COMPRESS_VALUE)) {
            hasCompression = true;
        }

        String keySchema, valSchema;

        try {
            if(props.containsKey(BUILD_FORCE_SCHEMA_KEY)) {
                keySchema = props.get(BUILD_FORCE_SCHEMA_KEY);
            } else {
                Schema.Field keyField = schema.getField(keyFieldName);
                if (keyField == null) {
                    throw new VoldemortException("The configured key field (" + keyFieldName + ") was not found in the input data.");
                } else {
                    keySchema = "\n\t\t<type>" + serializerName + "</type>\n\t\t<schema-info version=\"0\">"
                            + keyField.schema() + "</schema-info>\n\t";
                }
            }
        } catch (VoldemortException e) {
            throw e;
        } catch (Exception e) {
            throw new VoldemortException("Error while trying to extract the key field", e);
        }

        try {
            if(props.containsKey(BUILD_FORCE_SCHEMA_VALUE)) {
                valSchema = props.get(BUILD_FORCE_SCHEMA_VALUE);
            } else {
                Schema.Field valueField = schema.getField(valueFieldName);
                if (valueField == null) {
                    throw new VoldemortException("The configured value field (" + valueFieldName + ") was not found in the input data.");
                } else {
                    valSchema = "\n\t\t<type>" + serializerName + "</type>\n\t\t<schema-info version=\"0\">"
                            + valueField.schema() + "</schema-info>\n\t";

                    if(hasCompression) {
                        valSchema += "\t<compression><type>gzip</type></compression>\n\t";
                    }
                }
            }
        } catch (VoldemortException e) {
            throw e;
        } catch (Exception e) {
            throw new VoldemortException("Error while trying to extract the value field", e);
        }

        if (keySchema == null || valSchema == null) {
            // This should already have failed on previous exceptions, but just in case...
            throw new VoldemortException("There was a problem defining the key or value schema for this job.");
        } else {
            String newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                    replicationFactor,
                    requiredReads,
                    requiredWrites,
                    props.containsKey(BUILD_PREFERRED_READS) ? props.getInt(BUILD_PREFERRED_READS)
                            : null,
                    props.containsKey(BUILD_PREFERRED_WRITES) ? props.getInt(BUILD_PREFERRED_WRITES)
                            : null,
                    (props.containsKey(PUSH_FORCE_SCHEMA_KEY)) ? props.getString(PUSH_FORCE_SCHEMA_KEY)
                            : keySchema,
                    (props.containsKey(PUSH_FORCE_SCHEMA_VALUE)) ? props.getString(PUSH_FORCE_SCHEMA_VALUE)
                            : valSchema,
                    description,
                    owners);
            KeyValueSchema returnSchemaObj = new KeyValueSchema(keySchema, valSchema);
            boolean foundStore = findAndVerifyAvro(url,
                    newStoreDefXml,
                    hasCompression,
                    replicationFactor,
                    requiredReads,
                    requiredWrites,
                    serializerName,
                    returnSchemaObj);
            if (!foundStore) {
                try {
                    StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);
                    addStore(description, owners, url, newStoreDef);
                }
                catch(RuntimeException e) {
                    log.error("Error in adding store definition from: " + url, e);
                    throw new VoldemortException("Error in adding store definition from: " + url, e);
                }
            }
            // don't use newStoreDef because we want to ALWAYS use the JSON definition since the store
            // builder assumes that you are using JsonTypeSerializer. This allows you to tweak your
            // value/key store xml  as you see fit, but still uses the json sequence file meta data
            // to  build the store.
            storeDefs = ImmutableList.of(VoldemortUtils.getStoreDef(VoldemortUtils.getStoreDefXml(storeName,
                    replicationFactor,
                    requiredReads,
                    requiredWrites,
                    props.containsKey(BUILD_PREFERRED_READS) ? props.getInt(BUILD_PREFERRED_READS)
                            : null,
                    props.containsKey(BUILD_PREFERRED_WRITES) ? props.getInt(BUILD_PREFERRED_WRITES)
                            : null,
                    returnSchemaObj.keySchema,
                    returnSchemaObj.valSchema)));
            cluster = adminClientPerCluster.get(url).getAdminClientCluster();
        }
    }
 
    /**
     * Check if store exists and then verify the schema. Returns false if store doesn't exist
     * 
     * @param url to check
     * @param newStoreDefXml
     * @param hasCompression
     * @param replicationFactor
     * @param requiredReads
     * @param requiredWrites
     * @param serializerName
     * @param schemaObj key/value schema obj
     * @return boolean value true means store exists, false otherwise
     * 
     */
    private boolean findAndVerifyAvro(String url,
                                      String newStoreDefXml,
                                      boolean hasCompression,
                                      int replicationFactor,
                                      int requiredReads,
                                      int requiredWrites,
                                      String serializerName,
                                      KeyValueSchema schemaObj) {
        log.info("Verifying store: \n" + newStoreDefXml.toString());
        StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);
        // get store def from cluster
        log.info("Getting store definition from: " + url + " (node id " + this.nodeId + ")");
        List<StoreDefinition> remoteStoreDefs =
                adminClientPerCluster.get(url).metadataMgmtOps.getRemoteStoreDefList(this.nodeId).getValue();
        boolean foundStore = false;
        // go over all store defs and see if one has the same name as the store we're trying to build
        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(storeName)) {
                // if the store already exists, but doesn't match what we want to push, we need to worry
                if(!remoteStoreDef.equals(newStoreDef)) {
                    // let's check to see if the key/value serializers are
                    // REALLY equal.
                    SerializerDefinition localKeySerializerDef = newStoreDef.getKeySerializer();
                    SerializerDefinition localValueSerializerDef = newStoreDef.getValueSerializer();
                    SerializerDefinition remoteKeySerializerDef = remoteStoreDef.getKeySerializer();
                    SerializerDefinition remoteValueSerializerDef = remoteStoreDef.getValueSerializer();
                    if(remoteKeySerializerDef.getName().equals(serializerName)
                       && remoteValueSerializerDef.getName().equals(serializerName)) {

                        Schema remoteKeyDef = Schema.parse(remoteKeySerializerDef.getCurrentSchemaInfo());
                        Schema remoteValDef = Schema.parse(remoteValueSerializerDef.getCurrentSchemaInfo());
                        Schema localKeyDef = Schema.parse(localKeySerializerDef.getCurrentSchemaInfo());
                        Schema localValDef = Schema.parse(localValueSerializerDef.getCurrentSchemaInfo());

                        if(remoteKeyDef.equals(localKeyDef) && remoteValDef.equals(localValDef)) {

                            String compressionPolicy = "";
                            if(hasCompression) {
                                compressionPolicy = "\n\t\t<compression><type>gzip</type></compression>";
                            }

                            // if the key/value serializers are REALLY equal
                            // (even though the strings may not match), then
                            // just use the remote stores to GUARANTEE that
                            // they
                            // match, and try again.

                            String keySerializerStr = "\n\t\t<type>"
                                                      + remoteKeySerializerDef.getName()
                                                      + "</type>";

                            if(remoteKeySerializerDef.hasVersion()) {

                                Map<Integer, String> versions = new HashMap<Integer, String>();
                                for(Map.Entry<Integer, String> entry: remoteKeySerializerDef.getAllSchemaInfoVersions()
                                                                                            .entrySet()) {
                                    keySerializerStr += "\n\t\t <schema-info version=\""
                                                        + entry.getKey() + "\">"
                                                        + entry.getValue()
                                                        + "</schema-info>\n\t";
                                }

                            } else {
                                keySerializerStr = "\n\t\t<type>"
                                                   + serializerName
                                                   + "</type>\n\t\t<schema-info version=\"0\">"
                                                   + remoteKeySerializerDef.getCurrentSchemaInfo()
                                                   + "</schema-info>\n\t";
                            }

                            schemaObj.keySchema = keySerializerStr;
                            String valueSerializerStr = "\n\t\t<type>"
                                                        + remoteValueSerializerDef.getName()
                                                        + "</type>";

                            if(remoteValueSerializerDef.hasVersion()) {

                                Map<Integer, String> versions = new HashMap<Integer, String>();
                                for(Map.Entry<Integer, String> entry: remoteValueSerializerDef.getAllSchemaInfoVersions()
                                                                                              .entrySet()) {
                                    valueSerializerStr += "\n\t\t <schema-info version=\""
                                                          + entry.getKey() + "\">"
                                                          + entry.getValue()
                                                          + "</schema-info>\n\t";
                                }
                                valueSerializerStr += compressionPolicy + "\n\t";

                            } else {

                                valueSerializerStr = "\n\t\t<type>"
                                                     + serializerName
                                                     + "</type>\n\t\t<schema-info version=\"0\">"
                                                     + remoteValueSerializerDef.getCurrentSchemaInfo()
                                                     + "</schema-info>" + compressionPolicy
                                                     + "\n\t";

                            }
                            schemaObj.valSchema = valueSerializerStr;

                            newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                                           replicationFactor,
                                                                           requiredReads,
                                                                           requiredWrites,
                                                                           props.containsKey(BUILD_PREFERRED_READS) ? props.getInt(BUILD_PREFERRED_READS)
                                                                                                                     : null,
                                                                           props.containsKey(BUILD_PREFERRED_WRITES) ? props.getInt(BUILD_PREFERRED_WRITES)
                                                                                                                      : null,
                                                                           keySerializerStr,
                                                                           valueSerializerStr);

                            newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

                            if(!remoteStoreDef.equals(newStoreDef)) {
                                // if we still get a fail, then we know that the store defs don't match for reasons
                                // OTHER than the key/value serializer
                                throw new RuntimeException("Your store schema is identical, but the store definition does not match. Have: "
                                                           + newStoreDef
                                                           + "\nBut expected: "
                                                           + remoteStoreDef);
                            }

                        } else {
                            // if the key/value serializers are not equal (even in java, not just json strings),
                            // then fail
                            throw new RuntimeException("Your store definition does not match the store definition that is already in the cluster. Tried to resolve identical schemas between local and remote, but failed. Have: "
                                                       + newStoreDef
                                                       + "\nBut expected: "
                                                       + remoteStoreDef);
                        }
                    } else {
                        throw new RuntimeException("Your store definition does not match the store definition that is already in the cluster. Have: "
                                                   + newStoreDef
                                                   + "\nBut expected: "
                                                   + remoteStoreDef);
                    }
                }
                foundStore = true;
                break;
            }
        }
        return foundStore;
    }

    private class HeartBeatHookRunnable implements Runnable {
        final int sleepTimeMs;
        boolean keepRunning = true;

        HeartBeatHookRunnable(int sleepTimeMs) {
            this.sleepTimeMs = sleepTimeMs;
        }

        public void stop() {
            keepRunning = false;
        }

        public void run() {
            while (keepRunning) {
                try {
                    Thread.sleep(sleepTimeMs);
                    invokeHooks(BuildAndPushStatus.HEARTBEAT);
                } catch (InterruptedException e) {
                    keepRunning = false;
                }
            }
        }
    }
}

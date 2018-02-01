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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.BootstrapFailureException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.disk.HadoopStoreWriter;
import voldemort.store.readonly.disk.KeyValueWriter;
import voldemort.store.readonly.hooks.BuildAndPushHook;
import voldemort.store.readonly.hooks.BuildAndPushStatus;
import voldemort.store.readonly.mr.AbstractStoreBuilderConfigurable;
import voldemort.store.readonly.mr.AvroStoreBuilderMapper;
import voldemort.store.readonly.mr.HadoopStoreBuilder;
import voldemort.store.readonly.mr.JsonStoreBuilderMapper;
import voldemort.store.readonly.mr.serialization.JsonSequenceFileInputFormat;
import voldemort.store.readonly.mr.utils.AvroUtils;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.JsonSchema;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.DeleteAllFailedFetchStrategy;
import voldemort.store.readonly.swapper.DisableStoreOnFailedNodeFailedFetchStrategy;
import voldemort.store.readonly.swapper.FailedFetchStrategy;
import voldemort.store.readonly.swapper.RecoverableFailedFetchException;
import voldemort.utils.ExceptionUtils;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;
import azkaban.jobExecutor.AbstractJob;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.UninitializedMessageException;

public class VoldemortBuildAndPushJob extends AbstractJob {

    private final Logger log;

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
    public final static String BUILD_PRIMARY_REPLICAS_ONLY = "build.primary.replicas.only";
    // push.required
    public final static String PUSH_STORE_NAME = "push.store.name";
    public final static String PUSH_CLUSTER = "push.cluster";
    public final static String PUSH_STORE_OWNERS = "push.store.owners";
    public final static String PUSH_STORE_DESCRIPTION = "push.store.description";
    public final static String ENABLE_STORE_CREATION = "enable.store.creation";
    // push.optional
    public final static String PUSH_HTTP_TIMEOUT_SECONDS = "push.http.timeout.seconds";
    public final static String PUSH_VERSION = "push.version";
    public final static String PUSH_VERSION_TIMESTAMP = "push.version.timestamp";
    public final static String PUSH_BACKOFF_DELAY_SECONDS = "push.backoff.delay.seconds";
    public final static String PUSH_ROLLBACK = "push.rollback";
    public final static String PUSH_FORCE_SCHEMA_KEY = "push.force.schema.key";
    public final static String PUSH_FORCE_SCHEMA_VALUE = "push.force.schema.value";
    public final static String PUSH_CDN_CLUSTER = "push.cdn.cluster";   // e.g. "tcp://v-cluster1:6666|hdfs://cdn1:9000,tcp://v-cluster2:6666|webhdfs://cdn2:50070,tcp://v-cluster3:6666|null"
    public final static String PUSH_CDN_PREFIX = "push.cdn.prefix";     // e.g. "/jobs/VoldemortBnP"
    public final static String PUSH_CDN_ENABLED = "push.cdn.enabled";   // e.g. "true"
    public final static String PUSH_CDN_READ_BY_GROUP = "push.cdn.readByGroup";   // e.g. "true"
    public final static String PUSH_CDN_READ_BY_OTHER = "push.cdn.readByOther";   // e.g. "true"
    public final static String PUSH_CDN_WRITTEN_BY_GROUP = "push.cdn.writtenByGroup";   // e.g. "true"
    public final static String PUSH_CDN_WRITTEN_BY_OTHER = "push.cdn.writtenByOther";   // e.g. "true"
    public final static String PUSH_CDN_STORE_WHITELIST = "push.cdn.storeWhitelist";    // "storename1,storename2" no whitespace
    // others.optional
    public final static String KEY_SELECTION = "key.selection";
    public final static String VALUE_SELECTION = "value.selection";
    public final static String BUILD = "build";
    public final static String PUSH = "push";
    public final static String FETCH_ALL_STORES_XML = "fetch.all.stores.xml";
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
    public final static String REDUCER_OUTPUT_COMPRESS = "reducer.output.compress";
    public final static String STORE_VERIFICATION_MAX_THREAD_NUM = "store.verification.max.thread.num";
    public final static String ADMIN_CLIENT_CONNECTION_TIMEOUT_SEC = "admin.client.connection.timeout.sec";
    public final static String ADMIN_CLIENT_SOCKET_TIMEOUT_SEC = "admin.client.socket.timeout.sec";
    // default
    private final static String RECOMMENDED_FETCHER_PROTOCOL = "webhdfs";
    private final int DEFAULT_THREAD_NUM_FOR_STORE_VERIFICATION = 20;

    // CONFIG VALUES (and other immutable state)
    private final Props props;
    private final String storeName;
    private final List<String> clusterURLs;
    private final Map<String, AdminClient> adminClientPerCluster;
    private final List<String> dataDirs;
    private final boolean isAvroJob;
    private final String keyFieldName;
    private final String valueFieldName;
    private final boolean isAvroVersioned;
    private final long minNumberOfRecords;
    private final String hdfsFetcherPort;
    private final String hdfsFetcherProtocol;
    private final String jsonKeyField;
    private final String jsonValueField;
    private final String description;
    private final String owners;
    private final Set<BuildAndPushHook> hooks = new HashSet<BuildAndPushHook>();
    private final int heartBeatHookIntervalTime;
    private final HeartBeatHookRunnable heartBeatHookRunnable;
    private final boolean pushHighAvailability;
    private final List<Closeable> closeables = Lists.newArrayList();
    private final ExecutorService executorService;
    private final boolean enableStoreCreation;
    // Executor to do store/schema verification in parallel
    private final int maxThreadNumForStoreVerification;
    private ExecutorService storeVerificationExecutorService;
    private final HashMap<String, Exception> exceptions = Maps.newHashMap();

    // Mutable state
    private StoreDefinition storeDef;
    private Path sanitizedInputPath = null;
    private Schema inputPathAvroSchema = null;
    private JsonSchema inputPathJsonSchema = null;
    private Future heartBeatHookFuture = null;
    private Map<String, VAdminProto.GetHighAvailabilitySettingsResponse> haSettingsPerCluster;
    private boolean buildPrimaryReplicasOnly;

    private AdminClient createAdminClient(String url, boolean fetchAllStoresXml, int connectionTimeoutSec, int socketTimeoutSec) {
        ClientConfig config = new ClientConfig().setBootstrapUrls(url)
                .setConnectionTimeout(connectionTimeoutSec ,TimeUnit.SECONDS)
                .setFetchAllStoresXmlInBootstrap(fetchAllStoresXml);

        AdminClientConfig adminConfig = new AdminClientConfig().setAdminSocketTimeoutSec(socketTimeoutSec);
        return new AdminClient(adminConfig, config);
    }

    public VoldemortBuildAndPushJob(String name, azkaban.utils.Props azkabanProps) throws Exception {
        super(name, Logger.getLogger(name));
        this.log = getLog();

        this.props = new Props(azkabanProps.toProperties());
        log.info("Job props:\n" + this.props.toString(true));

        this.storeName = props.getString(PUSH_STORE_NAME).trim();
        final int connectionTimeoutSec = props.getInt(ADMIN_CLIENT_CONNECTION_TIMEOUT_SEC, 15);
        final int socketTimeoutSec = props.getInt(ADMIN_CLIENT_SOCKET_TIMEOUT_SEC, 180);
        this.clusterURLs = new ArrayList<String>();
        this.dataDirs = new ArrayList<String>();
        this.adminClientPerCluster = Maps.newHashMap();

        String clusterUrlText = props.getString(PUSH_CLUSTER);
        boolean fetchAllStoresXml = props.getBoolean(FETCH_ALL_STORES_XML, true);
        for(String url: Utils.COMMA_SEP.split(clusterUrlText.trim())) {
            if(url.trim().length() > 0) {
                if (clusterURLs.contains(url)) {
                    throw new VoldemortException("the URL: " + url + " is repeated in the "+ PUSH_CLUSTER + " property ");
                }
                try {
                    AdminClient adminClient = createAdminClient(url, fetchAllStoresXml, connectionTimeoutSec, socketTimeoutSec);
                    this.clusterURLs.add(url);
                    this.adminClientPerCluster.put(url, adminClient);
                    this.closeables.add(adminClient);
                } catch (Exception e) {
                    if (ExceptionUtils.recursiveClassEquals(e, BootstrapFailureException.class)) {
                        this.log.error("Unable to reach cluster: " + url + " ... this cluster will be skipped.", e);

                        // We add the exception in the exceptions map so that the job fails after pushing to the healthy clusters
                        exceptions.put(url, new VoldemortException(
                            "Unable to reach cluster: " + url + " ... that cluster was not pushed to.", e));
                    } else {
                        throw e;
                    }
                }
            }
        }

        int numberOfClusters = this.clusterURLs.size();

        if (numberOfClusters <= 0) {
            throw new RuntimeException("Number of URLs should be at least 1");
        }

        // Support multiple output dirs if the user mentions only PUSH, no BUILD.
        // If user mentions both then should have only one
        String dataDirText = props.getString(BUILD_OUTPUT_DIR);
        for(String dataDir: Utils.COMMA_SEP.split(dataDirText.trim()))
            if(dataDir.trim().length() > 0)
                this.dataDirs.add(dataDir);

        if(this.dataDirs.size() <= 0)
            throw new RuntimeException("Number of data dirs should be at least 1");


        this.hdfsFetcherProtocol = props.getString(VOLDEMORT_FETCHER_PROTOCOL, RECOMMENDED_FETCHER_PROTOCOL);
        if (!this.hdfsFetcherProtocol.equals(RECOMMENDED_FETCHER_PROTOCOL)) {
            log.warn("It is recommended to use the " + RECOMMENDED_FETCHER_PROTOCOL + " protocol only.");
        }

        this.hdfsFetcherPort = props.getString(VOLDEMORT_FETCHER_PORT, "50070");

        log.info(VOLDEMORT_FETCHER_PROTOCOL + " is set to : " + hdfsFetcherProtocol);
        log.info(VOLDEMORT_FETCHER_PORT + " is set to : " + hdfsFetcherPort);

        // Serialization configs

        isAvroJob = props.getBoolean(BUILD_TYPE_AVRO, false);
        // Set default to false, this ensures existing clients who are not aware of
        // the new serializer type don't bail out
        this.isAvroVersioned = props.getBoolean(AVRO_SERIALIZER_VERSIONED, false);
        this.keyFieldName = props.getString(AVRO_KEY_FIELD, null);
        this.valueFieldName = props.getString(AVRO_VALUE_FIELD, null);
        if(this.isAvroJob) {
            if(this.keyFieldName == null)
                throw new RuntimeException("The key field must be specified in the properties for the Avro build and push job!");
            if(this.valueFieldName == null)
                throw new RuntimeException("The value field must be specified in the properties for the Avro build and push job!");
        }
        this.jsonKeyField = props.getString(KEY_SELECTION, null);
        this.jsonValueField = props.getString(VALUE_SELECTION, null);

        // Other configs

        this.minNumberOfRecords = props.getLong(MIN_NUMBER_OF_RECORDS, 1);

        this.description = props.getString(PUSH_STORE_DESCRIPTION, "");
        if (description.length() == 0) {
            throw new RuntimeException("Description field missing in store definition. " +
                                       "Please add \"" + PUSH_STORE_DESCRIPTION +
                                       "\" with a line describing your store");
        }
        this.owners = props.getString(PUSH_STORE_OWNERS, "");
        if (owners.length() == 0) {
            throw new RuntimeException("Owner field missing in store definition. " +
                                       "Please add \"" + PUSH_STORE_OWNERS +
                                       "\" with value being a comma-separated list of email addresses.");

        }

        // By default, Push HA will be enabled if the server says so.
        // If the job sets Push HA to false, then it will be disabled, no matter what the server asks for.
        this.pushHighAvailability = props.getBoolean(VoldemortConfig.PUSH_HA_ENABLED, true);

        //By default, BnP plugin is able to create new store during the push if sotres are not found at the cluster.
        this.enableStoreCreation = props.getBoolean(ENABLE_STORE_CREATION, true);

        // Initializing hooks
        this.heartBeatHookIntervalTime = props.getInt(HEARTBEAT_HOOK_INTERVAL_MS, 60000);
        this.heartBeatHookRunnable = new HeartBeatHookRunnable(heartBeatHookIntervalTime);
        String hookNamesText = this.props.getString(HOOKS, null);
        if (hookNamesText != null && !hookNamesText.isEmpty()) {
            for (String hookName : Utils.COMMA_SEP.split(hookNamesText.trim())) {
                try {
                    BuildAndPushHook hook = (BuildAndPushHook) ReflectUtils.callConstructor(Class.forName(hookName));
                    try {
                        hook.init(props);
                        log.info("Initialized BuildAndPushHook [" + hook.getName() + "]");
                        this.hooks.add(hook);
                    } catch (Exception e) {
                        log.warn("Failed to initialize BuildAndPushHook [" + hook.getName() + "]. It will not be invoked.", e);
                    }
                } catch (ClassNotFoundException e) {
                    log.error("The requested BuildAndPushHook [" + hookName + "] was not found! Check your classpath and config!", e);
                }
            }
        }

        int requiredNumberOfThreads = numberOfClusters;
        if (hooks.size() > 0) {
            requiredNumberOfThreads++;
        }
        this.executorService = Executors.newFixedThreadPool(requiredNumberOfThreads);

        this.maxThreadNumForStoreVerification = props.getInt(STORE_VERIFICATION_MAX_THREAD_NUM,
            DEFAULT_THREAD_NUM_FOR_STORE_VERIFICATION);
        // Specifying value <= 1 for prop: STORE_VERIFICATION_MAX_THREAD_NUM will enable sequential store verification.
        if (this.maxThreadNumForStoreVerification > 1) {
            this.storeVerificationExecutorService = Executors.newFixedThreadPool(this.maxThreadNumForStoreVerification);
            log.info("Build and Push Job will run store verification in parallel, thread num: " + this.maxThreadNumForStoreVerification);
        } else {
            log.info("Build and Push Job will run store verification sequentially.");
        }

        log.info("Build and Push Job constructed for " + numberOfClusters + " cluster(s).");
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
        // There is no way for us to support pushing to multiple clusters with
        // different numbers of partitions from a single BnP job.
        if (lhs.getNumberOfPartitions() != rhs.getNumberOfPartitions())
            return false;

        if (buildPrimaryReplicasOnly) {
            // In 'build.primary.replicas.only' mode, we can support pushing to
            // clusters with different number of nodes and different partition
            // assignments.
            return true;
        }

        // Otherwise, we need every corresponding node in each cluster to be identical.
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
        Validate.notEmpty(clusterUrls, "clusterUrls cannot be null");
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
                                             + " is not the same as " + clusterRhs.getName());
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
                     + " just one job with both build and push set as true and pass a list of cluster urls.");
        }
    }

    /**
     * This function takes care of interrogating the servers to know which optional
     * features are supported and enabled, including:
     *
     *      1) block-level compression,
     *      2) high-availability push,
     *      3) build primary replicas only.
     *
     * TODO: Eventually, it'd be nice to migrate all of these code paths to the new simpler API:
     * {@link AdminClient.MetadataManagementOperations#getServerConfig(int, java.util.Set)}
     *
     * This function mutates the internal state of the job accordingly.
     */
    private void negotiateJobSettingsWithServers() {
        // 1. Get block-level compression settings

        // FIXME: Currently this code requests only one cluster for its supported compression codec.
        log.info("Requesting block-level compression codec expected by Server");
        String chosenCodec = null;
        List<String> supportedCodecs;
        try{
            supportedCodecs = adminClientPerCluster.get(clusterURLs.get(0))
                    .readonlyOps.getSupportedROStorageCompressionCodecs();

            String codecList = "[ ";
            for(String str: supportedCodecs) {
                codecList += str + " ";
            }
            codecList += "]";
            log.info("Server responded with block-level compression codecs: " + codecList);
            /*
             * TODO for now only checking if there is a match between the server
             * supported codec and the one that we support. Later this method can be
             * extended to add more compression types or pick the first type
             * returned by the server.
             */

            for(String codecStr: supportedCodecs) {
                if(codecStr.toUpperCase(Locale.ENGLISH).equals(KeyValueWriter.COMPRESSION_CODEC)) {
                    chosenCodec = codecStr;
                    break;
                }
            }
        } catch(Exception e) {
            log.error("Exception thrown when requesting for supported block-level compression codecs. " +
                      "Server might be running in a older version. Exception: " + e.getMessage());
            // We will continue without block-level compression enabled
        }

        if(chosenCodec != null) {
            log.info("Using block-level compression codec: " + chosenCodec);
            this.props.put(REDUCER_OUTPUT_COMPRESS, "true");
            this.props.put(REDUCER_OUTPUT_COMPRESS_CODEC, chosenCodec);
        } else {
            log.info("Using no block-level compression");
        }

        // 2. Get High-Availability settings

        this.haSettingsPerCluster = Maps.newHashMap();
        if (!pushHighAvailability) {
            log.info("pushHighAvailability is disabled by the job config.");
        } else {
            // HA is enabled by the BnP job config
            for (String clusterUrl: clusterURLs) {
                try {
                    VAdminProto.GetHighAvailabilitySettingsResponse serverSettings =
                            adminClientPerCluster.get(clusterUrl).readonlyOps.getHighAvailabilitySettings();
                    this.haSettingsPerCluster.put(clusterUrl, serverSettings);
                } catch (UninitializedMessageException e) {
                    // Not printing out the exception in the logs as that is a benign error.
                    log.error("The server does not support HA (introduced in release 1.9.20), so " +
                              "pushHighAvailability will be DISABLED on cluster: " + clusterUrl);
                } catch (Exception e) {
                    log.error("Got exception while trying to determine pushHighAvailability settings on cluster: " + clusterUrl, e);
                }
            }
        }

        // 3. Get "build.primary.replicas.only" setting

        Map<String, String> expectedConfig = Maps.newHashMap();
        expectedConfig.put(VoldemortConfig.READONLY_BUILD_PRIMARY_REPLICAS_ONLY, Boolean.toString(true));
        this.buildPrimaryReplicasOnly = true;
        for (String clusterUrl: clusterURLs) {
            VAdminProto.GetHighAvailabilitySettingsResponse serverSettings = haSettingsPerCluster.get(clusterUrl);
            int maxNodeFailuresForCluster = 0;
            if (serverSettings != null) {
                maxNodeFailuresForCluster = serverSettings.getMaxNodeFailure();
            }
            if (!adminClientPerCluster.get(clusterUrl).metadataMgmtOps.validateServerConfig(expectedConfig, maxNodeFailuresForCluster)) {
                log.info("'" + BUILD_PRIMARY_REPLICAS_ONLY + "' is not supported on this destination cluster: " + clusterUrl);
                this.buildPrimaryReplicasOnly = false;
            }
        }
    }

    private class StorePushTask implements Callable<Boolean> {
        final Props props;
        final String url;
        final String buildOutputDir;

        StorePushTask(Props props, String url, String buildOutputDir) {
            this.props = props;
            this.url = url;
            this.buildOutputDir = buildOutputDir;

            log.debug("StorePushTask constructed for cluster URL: " + url);
        }

        public Boolean call() throws Exception {
            log.info("StorePushTask.call() invoked for cluster URL: " + url);
            invokeHooks(BuildAndPushStatus.PUSHING, url);
            try {
                runPushStore(props, url, buildOutputDir);
                log.info("StorePushTask.call() finished for cluster URL: " + url);
                invokeHooks(BuildAndPushStatus.SWAPPED, url);
            } catch (RecoverableFailedFetchException e) {
                log.warn("There was a problem with some of the fetches, but a swap was still able " +
                                 "to go through for cluster URL: " + url, e);
                invokeHooks(BuildAndPushStatus.SWAPPED_WITH_FAILURES, url);
            } catch(Exception e) {
                log.error("Exception during push for cluster URL: " + url + ". Rethrowing exception.");
                throw e;
            }
            return true;
        }
    }

    @Override
    public void run() throws Exception {
        invokeHooks(BuildAndPushStatus.STARTING);
        if (hooks.size() > 0) {
            heartBeatHookFuture = executorService.submit(heartBeatHookRunnable);
        }

        try {
            // These two options control the build and push phases of the job respectively.
            boolean build = props.getBoolean(BUILD, true);
            boolean push = props.getBoolean(PUSH, true);
            checkForPreconditions(build, push);

            negotiateJobSettingsWithServers();

            try {
                // The cluster equality check is performed after negotiating job settings
                // with the servers because the constraints are relaxed if the servers
                // support/enable the 'build.primary.replicas.only' mode.
                allClustersEqual(clusterURLs);
            } catch(VoldemortException e) {
                log.error("Exception during cluster equality check", e);
                fail("Exception during cluster equality check: " + e.toString());
                throw e;
            }

            // Create a hashmap to capture exception per url
            String buildOutputDir = null;
            Map<String, Future<Boolean>> tasks = Maps.newHashMap();
            for (int index = 0; index < clusterURLs.size(); index++) {
                String url = clusterURLs.get(index);
                if (isAvroJob) {
                    // Verify the schema if the store exists or else add the new store
                    verifyOrAddAvroStore(url, isAvroVersioned);
                } else {
                    // Verify the schema if the store exists or else add the new store
                    verifyOrAddJsonStore(url);
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
                            log.error("Exception during build for URL: " + url, e);
                            exceptions.put(url, e);
                        }
                    }
                }
                if (push) {
                    log.info("Pushing to cluster URL: " + clusterURLs.get(index));
                    // If we are not building and just pushing then we want to get the built
                    // from the dataDirs, or else we will just the one that we built earlier
                    if (!build) {
                        buildOutputDir = dataDirs.get(index);
                    }
                    // If there was an exception during the build part the buildOutputDir might be null, check
                    // if that's the case, if yes then continue and don't even try pushing
                    if (buildOutputDir == null) {
                        continue;
                    }
                    tasks.put(url, executorService.submit(new StorePushTask(props, url, buildOutputDir)));
                }
            }
            // We can safely shutdown storeVerificationExecutorService here since all the verifications are done.
            if (null != storeVerificationExecutorService) {
                storeVerificationExecutorService.shutdownNow();
                storeVerificationExecutorService = null;
            }

            for (Map.Entry<String, Future<Boolean>> task: tasks.entrySet()) {
                String url = task.getKey();
                Boolean success = false;
                try {
                    success = task.getValue().get();
                } catch(Exception e) {
                    exceptions.put(url, e);
                }
                if (success) {
                    log.info("Successfully pushed to cluster URL: " + url);
                }
            }

            if(build && push && buildOutputDir != null
               && !props.getBoolean(BUILD_OUTPUT_KEEP, false)) {
                JobConf jobConf = new JobConf();
                if(props.containsKey(HADOOP_JOB_UGI)) {
                    jobConf.set(HADOOP_JOB_UGI, props.getString(HADOOP_JOB_UGI));
                }
                log.info("Cleaning up: Deleting BnP output and temp files from HDFS: " + buildOutputDir);
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
            fail(e.toString());
            throw new VoldemortException("An exception occurred during Build and Push !!", e);
        } catch (Throwable t) {
            // This is for OOMs, StackOverflows and other uber nasties...
            // We'll try to invoke hooks but all bets are off at this point :/
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
        if (heartBeatHookFuture != null) {
            heartBeatHookFuture.cancel(true);
        }
        for (Closeable closeable: this.closeables) {
            try {
                log.info("Closing " + closeable.toString());
                closeable.close();
            } catch (Exception e) {
                log.error("Got an error while trying to close " + closeable.toString(), e);
            }
        }
        this.executorService.shutdownNow();
        if (null != this.storeVerificationExecutorService) {
            this.storeVerificationExecutorService.shutdownNow();
        }
    }

    public String runBuildStore(Props props, String url) throws Exception {
        Path tempDir = new Path(props.getString(BUILD_TEMP_DIR, "/tmp/vold-build-and-push-" + new Random().nextLong()));
        Path outputDir = new Path(props.getString(BUILD_OUTPUT_DIR), new URI(url).getHost());
        CheckSumType checkSumType = CheckSum.fromString(props.getString(CHECKSUM_TYPE,
                                                                        CheckSum.toString(CheckSumType.MD5)));
        JobConf configuration = new JobConf();
        Class mapperClass;
        Class<? extends InputFormat> inputFormatClass;

        // Only if its a avro job we supply some additional fields
        // for the key value schema of the avro record
        if(this.isAvroJob) {
            configuration.set(HadoopStoreBuilder.AVRO_REC_SCHEMA, getRecordSchema());
            configuration.set(AvroStoreBuilderMapper.AVRO_KEY_SCHEMA, getKeySchema());
            configuration.set(AvroStoreBuilderMapper.AVRO_VALUE_SCHEMA, getValueSchema());
            configuration.set(VoldemortBuildAndPushJob.AVRO_KEY_FIELD, this.keyFieldName);
            configuration.set(VoldemortBuildAndPushJob.AVRO_VALUE_FIELD, this.valueFieldName);
            mapperClass = AvroStoreBuilderMapper.class;
            inputFormatClass = AvroInputFormat.class;
        } else {
            mapperClass = JsonStoreBuilderMapper.class;
            inputFormatClass = JsonSequenceFileInputFormat.class;
        }

        if (props.containsKey(AbstractStoreBuilderConfigurable.NUM_CHUNKS)) {
            log.warn("N.B.: The '" + AbstractStoreBuilderConfigurable.NUM_CHUNKS + "' config parameter is now " +
                     "deprecated and ignored. The BnP job will automatically determine a proper value for this setting.");
        }

        HadoopStoreBuilder builder = new HadoopStoreBuilder(getId() + "-build-store",
                                                            props,
                                                            configuration,
                                                            mapperClass,
                                                            inputFormatClass,
                                                            this.adminClientPerCluster.get(url).getAdminClientCluster(),
                                                            this.storeDef,
                                                            tempDir,
                                                            outputDir,
                                                            getInputPath(),
                                                            checkSumType,
                                                            props.getBoolean(SAVE_KEYS, true),
                                                            props.getBoolean(REDUCER_PER_BUCKET, true),
                                                            props.getInt(BUILD_CHUNK_SIZE,
                                                                (int) HadoopStoreWriter.DEFAULT_CHUNK_SIZE),
                                                            this.isAvroJob,
                                                            this.minNumberOfRecords,
                                                            this.buildPrimaryReplicasOnly);

        builder.build();

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
        int maxNodeFailures = 0; // Default (when BnP HA is not enabled) is potentially overridden below.

        if (pushHighAvailability) {
            // HA is enabled by the BnP job config
            VAdminProto.GetHighAvailabilitySettingsResponse serverSettings = haSettingsPerCluster.get(url);

            if (serverSettings == null || !serverSettings.getEnabled()) {
                log.warn("pushHighAvailability is DISABLED on cluster: " + url);
            } else {
                // HA is enabled by the server config
                maxNodeFailures = serverSettings.getMaxNodeFailure();
                OutputStream outputStream = new ByteArrayOutputStream();
                props.storeFlattened(outputStream);
                outputStream.flush();
                String jobInfoString = outputStream.toString();
                failedFetchStrategyList.add(
                        new DisableStoreOnFailedNodeFailedFetchStrategy(
                                adminClientPerCluster.get(url),
                                jobInfoString));
                log.info("pushHighAvailability is enabled for cluster URL: " + url +
                        " with cluster ID: " + serverSettings.getClusterId());
            }
        }

        boolean rollback = props.getBoolean(PUSH_ROLLBACK, true);

        if (rollback) {
            failedFetchStrategyList.add(
                    new DeleteAllFailedFetchStrategy(adminClientPerCluster.get(url)));
        }

        Cluster cluster = adminClientPerCluster.get(url).getAdminClientCluster();

        log.info("Push starting for cluster: " + url);

        // CDN distcp
        boolean cdnEnabled = props.getBoolean(PUSH_CDN_ENABLED, false);
        String modifiedDataDir = new Path(dataDir).makeQualified(FileSystem.get(new JobConf())).toString();
        List storeWhitelist = props.getList(PUSH_CDN_STORE_WHITELIST, null);
        GobblinDistcpJob distcpJob = null;

        if (cdnEnabled && storeWhitelist != null && storeWhitelist.contains(storeName)) {
            try {
                if (modifiedDataDir.matches(".*hdfs://.*:[0-9]{1,5}/.*")) {
                    invokeHooks(BuildAndPushStatus.DISTCP_STARTING, url);
                    distcpJob = new GobblinDistcpJob(getId(), modifiedDataDir, url, props);
                    distcpJob.run();
                    modifiedDataDir = distcpJob.getSource();
                    invokeHooks(BuildAndPushStatus.DISTCP_FINISHED, url);
                } else {
                    warn("Invalid URL format! Skip Distcp.");
                    throw new RuntimeException("Invalid URL format! Skip Distcp.");
                }
            } catch (Exception e) {
                invokeHooks(BuildAndPushStatus.DISTCP_FAILED, url + " : " + e.getMessage());
            }
        }

        new VoldemortSwapJob(
                this.getId() + "-push-store",
                cluster,
                dataDir,
                storeName,
                httpTimeoutMs,
                pushVersion,
                maxBackoffDelayMs,
                rollback,
                hdfsFetcherProtocol,
                hdfsFetcherPort,
                maxNodeFailures,
                failedFetchStrategyList,
                url,
                modifiedDataDir,
                buildPrimaryReplicasOnly).run();

        if (distcpJob != null) {
            // This would allow temp dirs marked by deleteDirOnExit() to be deleted.
            distcpJob.closeCdnFS();
        }
    }

    /**
     * Get the sanitized input path. At the moment of writing, this means the
     * #LATEST tag is expanded.
     */
    private synchronized Path getInputPath() throws IOException {
        if (sanitizedInputPath == null) {
            // No need to query Hadoop more than once as this shouldn't change mid-run,
            // thus, we can lazily initialize and cache the result.
            Path path = new Path(props.getString(BUILD_INPUT_PATH));
            sanitizedInputPath = HadoopUtils.getSanitizedPath(path);
        }
        return sanitizedInputPath;
    }

    /**
     * Get the Json Schema of the input path, assuming the path contains just one
     * schema version in all files under that path.
     */
    private synchronized JsonSchema getInputPathJsonSchema() throws IOException {
        if (inputPathJsonSchema == null) {
            // No need to query Hadoop more than once as this shouldn't change mid-run,
            // thus, we can lazily initialize and cache the result.
            inputPathJsonSchema = HadoopUtils.getSchemaFromPath(getInputPath());
        }
        return inputPathJsonSchema;
    }

    /**
     * Get the Avro Schema of the input path, assuming the path contains just one
     * schema version in all files under that path.
     */
    private synchronized Schema getInputPathAvroSchema() throws IOException {
        if (inputPathAvroSchema == null) {
            // No need to query Hadoop more than once as this shouldn't change mid-run,
            // thus, we can lazily initialize and cache the result.
            inputPathAvroSchema = AvroUtils.getAvroSchemaFromPath(getInputPath());
        }
        return inputPathAvroSchema;
    }

    // Get the schema for the Avro Record from the object container file
    public String getRecordSchema() throws IOException {
        Schema schema = getInputPathAvroSchema();
        String recSchema = schema.toString();
        return recSchema;
    }

    // Extract schema of the key field
    public String getKeySchema() throws IOException {
        Schema schema = getInputPathAvroSchema();
        String keySchema = schema.getField(keyFieldName).schema().toString();
        return keySchema;
    }

    // Extract schema of the value field
    public String getValueSchema() throws IOException {
        Schema schema = getInputPathAvroSchema();
        String valueSchema = schema.getField(valueFieldName).schema().toString();
        return valueSchema;
    }

    private void verifyOrAddJsonStore(String url) throws Exception {
        // create new json store def with schema from the metadata in the input path
        JsonSchema schema = getInputPathJsonSchema();
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

        if(props.getBoolean(BUILD_COMPRESS_VALUE, false)) {
            valSchema += "\t<compression><type>gzip</type></compression>\n\t";
        }

        verifyOrAddStore(url,
                         props.getString(BUILD_FORCE_SCHEMA_KEY, keySchema),
                         props.getString(BUILD_FORCE_SCHEMA_VALUE, valSchema));
    }

    public void verifyOrAddAvroStore(String url, boolean isVersioned) throws Exception {
        Schema schema = getInputPathAvroSchema();
        String serializerName;
        if (isVersioned)
            serializerName = DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME;
        else
            serializerName = DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME;

        String keySchema, valSchema;

        try {
            Schema.Field keyField = schema.getField(keyFieldName);
            if (keyField == null) {
                throw new VoldemortException("The configured key field (" + keyFieldName + ") was not found in the input data.");
            } else {
                keySchema = "\n\t\t<type>" + serializerName + "</type>\n\t\t<schema-info version=\"0\">"
                        + keyField.schema() + "</schema-info>\n\t";
            }
        } catch (VoldemortException e) {
            throw e;
        } catch (Exception e) {
            throw new VoldemortException("Error while trying to extract the key field", e);
        }

        try {
            Schema.Field valueField = schema.getField(valueFieldName);
            if (valueField == null) {
                throw new VoldemortException("The configured value field (" + valueFieldName + ") was not found in the input data.");
            } else {
                valSchema = "\n\t\t<type>" + serializerName + "</type>\n\t\t<schema-info version=\"0\">"
                        + valueField.schema() + "</schema-info>\n\t";

                if(props.getBoolean(BUILD_COMPRESS_VALUE, false)) {
                    valSchema += "\t<compression><type>gzip</type></compression>\n\t";
                }
            }
        } catch (VoldemortException e) {
            throw e;
        } catch (Exception e) {
            throw new VoldemortException("Error while trying to extract the value field", e);
        }

        verifyOrAddStore(url,
                         props.getString(BUILD_FORCE_SCHEMA_KEY, keySchema),
                         props.getString(BUILD_FORCE_SCHEMA_VALUE, valSchema));
    }

    /**
     * For each node, checks if the store exists and then verifies that the remote schema
     * matches the new one. If the remote store doesn't exist, it creates it.
     */
    private void verifyOrAddStore(String clusterURL,
                                  String keySchema,
                                  String valueSchema) {
        String newStoreDefXml = VoldemortUtils.getStoreDefXml(
                storeName,
                props.getInt(BUILD_REPLICATION_FACTOR, 2),
                props.getInt(BUILD_REQUIRED_READS, 1),
                props.getInt(BUILD_REQUIRED_WRITES, 1),
                props.getNullableInt(BUILD_PREFERRED_READS),
                props.getNullableInt(BUILD_PREFERRED_WRITES),
                props.getString(PUSH_FORCE_SCHEMA_KEY, keySchema),
                props.getString(PUSH_FORCE_SCHEMA_VALUE, valueSchema),
                description,
                owners);

        log.info("Verifying store against cluster URL: " + clusterURL + "\n" + newStoreDefXml.toString());
        StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

        try {
            adminClientPerCluster.get(clusterURL).storeMgmtOps.verifyOrAddStore(newStoreDef, "BnP config/data",
                enableStoreCreation, this.storeVerificationExecutorService);
        } catch (UnreachableStoreException e) {
            log.info("verifyOrAddStore() failed on some nodes for clusterURL: " + clusterURL + " (this is harmless).", e);
            // When we can't reach some node, we just skip it and won't create the store on it.
            // Next time BnP is run while the node is up, it will get the store created.
        } // Other exceptions need to bubble up!

        storeDef = newStoreDef;
    }

    private class HeartBeatHookRunnable implements Runnable {
        final int sleepTimeMs;

        HeartBeatHookRunnable(int sleepTimeMs) {
            this.sleepTimeMs = sleepTimeMs;
        }

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(sleepTimeMs);
                    invokeHooks(BuildAndPushStatus.HEARTBEAT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}

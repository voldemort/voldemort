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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.azkaban.VoldemortStoreBuilderJob.VoldemortStoreBuilderConf;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.JsonSchema;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.utils.Pair;
import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class VoldemortMultiStoreBuildAndPushJob extends AbstractJob {

    private final Logger log;

    private final Props props;

    private List<String> storeNames;

    private final List<String> clusterUrls;

    /**
     * The input directories to use on a per store name basis
     */
    private final HashMap<String, Path> inputDirsPerStore;

    /**
     * The final output that the output directory is stored in is as follows - [
     * outputDir ]/[ store_name ]/[ cluster_name ]
     */
    private final Path outputDir;

    /**
     * The node id to which we'll query and check if the stores already exists.
     * If the store doesn't exist, it creates it.
     */
    private final int nodeId;

    /**
     * Get the sanitized input path. At the moment of writing, this means the
     * #LATEST tag is expanded.
     */
    private Path getPath(String pathString) throws IOException {
        Path path = new Path(pathString);
        return HadoopUtils.getSanitizedPath(path);
    }

    public VoldemortMultiStoreBuildAndPushJob(String name, Props props) throws IOException {
        super(name);
        this.props = props;
        this.log = Logger.getLogger(name);
        this.nodeId = props.getInt("check.node", 0);

        // Get the input directories
        List<String> inputDirsPathString = VoldemortUtils.getCommaSeparatedStringValues(props.getString("build.input.path"),
                                                                                        "input directory");

        // Get the store names
        this.storeNames = VoldemortUtils.getCommaSeparatedStringValues(props.getString("push.store.name"),
                                                                       "store name");

        // Check if the number of stores = input directories ( obviously )
        if(this.storeNames.size() != inputDirsPathString.size()) {
            throw new RuntimeException("Number of stores ( " + this.storeNames.size()
                                       + " ) is not equal to number of input directories ( "
                                       + inputDirsPathString.size() + " )");
        }

        // Convert them to Path
        this.inputDirsPerStore = Maps.newHashMap();
        int index = 0;
        for(String inputDirPathString: inputDirsPathString) {
            this.inputDirsPerStore.put(storeNames.get(index), getPath(inputDirPathString));
            index++;
        }

        // Get the output directory
        String outputDirString = props.getString("build.output.dir",
                                                 "/tmp/voldemort-build-and-push-temp-"
                                                         + new Random().nextLong());
        this.outputDir = getPath(outputDirString);

        log.info("Storing output of all push jobs in " + this.outputDir);

        // Get the cluster urls to push to
        this.clusterUrls = VoldemortUtils.getCommaSeparatedStringValues(props.getString("push.cluster"),
                                                                        "cluster urls");

    }

    /**
     * Given the filesystem and a path recursively goes in and calculates the
     * size
     * 
     * @param fs Filesystem
     * @param path The root path whose length we need to calculate
     * @return The length in long
     * @throws IOException
     */
    public long sizeOfPath(FileSystem fs, Path path) throws IOException {
        long size = 0;
        FileStatus[] statuses = fs.listStatus(path);
        if(statuses != null) {
            for(FileStatus status: statuses) {
                if(status.isDir())
                    size += sizeOfPath(fs, status.getPath());
                else
                    size += status.getLen();
            }
        }
        return size;
    }

    @Override
    public void run() throws Exception {

        // Mapping of Pair [ cluster url, store name ] to List of previous node
        // directories.
        // Required for rollback...
        Multimap<Pair<String, String>, Pair<Integer, String>> previousNodeDirPerClusterStore = HashMultimap.create();

        // Retrieve filesystem information for checking if folder exists
        final FileSystem fs = outputDir.getFileSystem(new Configuration());

        // Step 1 ) Order the stores depending on the size of the store
        TreeMap<Long, String> storeNameSortedBySize = Maps.newTreeMap();
        for(String storeName: storeNames) {
            storeNameSortedBySize.put(sizeOfPath(fs, inputDirsPerStore.get(storeName)), storeName);
        }

        log.info("Store names along with their input file sizes - " + storeNameSortedBySize);

        // This will collect it in ascending order of size
        this.storeNames = Lists.newArrayList(storeNameSortedBySize.values());

        // Reverse it such that is in descending order of size
        Collections.reverse(this.storeNames);

        log.info("Store names in the order of which we'll run build and push - " + this.storeNames);

        // Step 2 ) Get the push version if set
        final long pushVersion = props.containsKey("push.version.timestamp") ? Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()))
                                                                            : props.getLong("push.version",
                                                                                            -1L);

        // Mapping of Pair [ cluster url, store name ] to Future with list of
        // node dirs
        HashMap<Pair<String, String>, Future<List<String>>> fetchDirsPerStoreCluster = Maps.newHashMap();

        // Store mapping of url to cluster metadata
        final ConcurrentHashMap<String, Cluster> urlToCluster = new ConcurrentHashMap<String, Cluster>();

        // Mapping of Pair [ cluster url, store name ] to List of node
        // directories
        final HashMap<Pair<String, String>, List<String>> nodeDirPerClusterStore = new HashMap<Pair<String, String>, List<String>>();

        // Iterate over all of them and check if they are complete
        final HashMap<Pair<String, String>, Exception> exceptions = Maps.newHashMap();

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(props.getInt("build.push.parallel", 1));

            // Step 3 ) Start the building + pushing of all stores in parallel
            for(final String storeName: storeNames) {
                // Go over every cluster and do the build phase
                for(int index = 0; index < clusterUrls.size(); index++) {
                    final String url = clusterUrls.get(index);
                    fetchDirsPerStoreCluster.put(Pair.create(url, storeName),
                                                 executor.submit(new Callable<List<String>>() {

                                                     @Override
                                                     public List<String> call() throws Exception {

                                                         log.info("========= Working on build + push phase for store '"
                                                                  + storeName
                                                                  + "' and cluster '"
                                                                  + url + "' ==========");

                                                         // Create an admin
                                                         // client which will be
                                                         // used by
                                                         // everyone
                                                         AdminClient adminClient = null;

                                                         // Executor inside
                                                         // executor - your mind
                                                         // just
                                                         // exploded!
                                                         ExecutorService internalExecutor = null;

                                                         try {
                                                             // Retrieve admin
                                                             // client for
                                                             // verification of
                                                             // schema + pushing
                                                             adminClient = new AdminClient(url,
                                                                                           new AdminClientConfig(),
                                                                                           new ClientConfig());

                                                             // Verify the store
                                                             // exists ( If not,
                                                             // add it
                                                             // the
                                                             // store )
                                                             Pair<StoreDefinition, Cluster> metadata = verifySchema(storeName,
                                                                                                                    url,
                                                                                                                    inputDirsPerStore.get(storeName),
                                                                                                                    adminClient);

                                                             // Populate the url
                                                             // to cluster
                                                             // metadata
                                                             urlToCluster.put(url,
                                                                              metadata.getSecond());

                                                             // Create output
                                                             // directory path
                                                             URI uri = new URI(url);

                                                             Path outputDirPath = new Path(outputDir
                                                                                                   + Path.SEPARATOR
                                                                                                   + storeName,
                                                                                           uri.getHost());

                                                             log.info("Running build phase for store '"
                                                                      + storeName
                                                                      + "' and url '"
                                                                      + url
                                                                      + "'. Reading from input directory '"
                                                                      + inputDirsPerStore.get(storeName)
                                                                      + "' and writing to "
                                                                      + outputDirPath);

                                                             runBuildStore(metadata.getSecond(),
                                                                           metadata.getFirst(),
                                                                           inputDirsPerStore.get(storeName),
                                                                           outputDirPath);

                                                             log.info("Finished running build phase for store "
                                                                      + storeName
                                                                      + " and url '"
                                                                      + url
                                                                      + "'. Written to directory "
                                                                      + outputDirPath);

                                                             long storePushVersion = pushVersion;
                                                             if(storePushVersion == -1L) {
                                                                 log.info("Retrieving version number for store '"
                                                                          + storeName
                                                                          + "' and cluster '"
                                                                          + url
                                                                          + "'");

                                                                 Map<String, Long> pushVersions = adminClient.readonlyOps.getROMaxVersion(Lists.newArrayList(storeName));

                                                                 if(pushVersions == null
                                                                    || !pushVersions.containsKey(storeName)) {
                                                                     throw new RuntimeException("Could not retrieve version for store '"
                                                                                                + storeName
                                                                                                + "'");
                                                                 }

                                                                 storePushVersion = pushVersions.get(storeName);
                                                                 storePushVersion++;

                                                                 log.info("Retrieved max version number for store '"
                                                                          + storeName
                                                                          + "' and cluster '"
                                                                          + url
                                                                          + "' = "
                                                                          + storePushVersion);
                                                             }

                                                             log.info("Running push for cluster url "
                                                                      + url);

                                                             // Used for
                                                             // parallel pushing
                                                             internalExecutor = Executors.newCachedThreadPool();

                                                             AdminStoreSwapper swapper = new AdminStoreSwapper(metadata.getSecond(),
                                                                                                               internalExecutor,
                                                                                                               adminClient,
                                                                                                               1000 * props.getInt("timeout.seconds",
                                                                                                                                   24 * 60 * 60),
                                                                                                               true,
                                                                                                               true);

                                                             // Convert to
                                                             // hadoop specific
                                                             // path
                                                             String outputDirPathString = outputDirPath.makeQualified(fs)
                                                                                                       .toString();

                                                             if(!fs.exists(outputDirPath)) {
                                                                 throw new RuntimeException("Output directory for store "
                                                                                            + storeName
                                                                                            + " and cluster '"
                                                                                            + url
                                                                                            + "' - "
                                                                                            + outputDirPathString
                                                                                            + " does not exist");
                                                             }

                                                             log.info("Pushing data to store '"
                                                                      + storeName + "' on cluster "
                                                                      + url + " from path  "
                                                                      + outputDirPathString
                                                                      + " with version "
                                                                      + storePushVersion);

                                                             List<String> nodeDirs = swapper.invokeFetch(storeName,
                                                                                                         outputDirPathString,
                                                                                                         storePushVersion);

                                                             log.info("Successfully pushed data to store '"
                                                                      + storeName
                                                                      + "' on cluster "
                                                                      + url
                                                                      + " from path  "
                                                                      + outputDirPathString
                                                                      + " with version "
                                                                      + storePushVersion);

                                                             return nodeDirs;
                                                         } finally {
                                                             if(internalExecutor != null) {
                                                                 internalExecutor.shutdownNow();
                                                                 internalExecutor.awaitTermination(10,
                                                                                                   TimeUnit.SECONDS);
                                                             }
                                                             if(adminClient != null) {
                                                                 adminClient.close();
                                                             }
                                                         }
                                                     }

                                                 }));

                }

            }

            for(final String storeName: storeNames) {
                for(int index = 0; index < clusterUrls.size(); index++) {
                    Pair<String, String> key = Pair.create(clusterUrls.get(index), storeName);
                    Future<List<String>> nodeDirs = fetchDirsPerStoreCluster.get(key);
                    try {
                        nodeDirPerClusterStore.put(key, nodeDirs.get());
                    } catch(Exception e) {
                        exceptions.put(key, e);
                    }
                }
            }

        } finally {
            if(executor != null) {
                executor.shutdownNow();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }
        }

        // ===== If we got exceptions during the build + push, delete data from
        // successful
        // nodes ======
        if(!exceptions.isEmpty()) {

            log.error("Got an exception during pushes. Deleting data already pushed on successful nodes");

            for(int index = 0; index < clusterUrls.size(); index++) {
                String clusterUrl = clusterUrls.get(index);
                Cluster cluster = urlToCluster.get(clusterUrl);

                AdminClient adminClient = null;
                try {
                    adminClient = new AdminClient(cluster,
                                                  new AdminClientConfig(),
                                                  new ClientConfig());
                    for(final String storeName: storeNames) {
                        // Check if the [ cluster , store name ] succeeded. We
                        // need to roll it back
                        Pair<String, String> key = Pair.create(clusterUrl, storeName);

                        if(nodeDirPerClusterStore.containsKey(key)) {
                            List<String> nodeDirs = nodeDirPerClusterStore.get(key);

                            log.info("Deleting data for successful pushes to " + clusterUrl
                                     + " and store " + storeName);
                            int nodeId = 0;
                            for(String nodeDir: nodeDirs) {
                                try {
                                    log.info("Deleting data ( " + nodeDir
                                             + " ) for successful pushes to '" + clusterUrl
                                             + "' and store '" + storeName + "' and node " + nodeId);
                                    adminClient.readonlyOps.failedFetchStore(nodeId,
                                                                             storeName,
                                                                             nodeDir);
                                    log.info("Successfully deleted data for successful pushes to '"
                                             + clusterUrl + "' and store '" + storeName
                                             + "' and node " + nodeId);

                                } catch(Exception e) {
                                    log.error("Failure while deleting data on node " + nodeId
                                              + " for store '" + storeName + "' and url '"
                                              + clusterUrl + "'");
                                }
                                nodeId++;
                            }
                        }
                    }
                } finally {
                    if(adminClient != null) {
                        adminClient.close();
                    }
                }
            }

            int errorNo = 1;
            for(Pair<String, String> key: exceptions.keySet()) {
                log.error("Error no " + errorNo + "] Error pushing for cluster '" + key.getFirst()
                          + "' and store '" + key.getSecond() + "' :", exceptions.get(key));
                errorNo++;
            }

            throw new VoldemortException("Exception during build + push");
        }

        // ====== Delete the temporary directory since we don't require it
        // ======
        if(!props.getBoolean("build.output.keep", false)) {
            JobConf jobConf = new JobConf();

            if(props.containsKey("hadoop.job.ugi")) {
                jobConf.set("hadoop.job.ugi", props.getString("hadoop.job.ugi"));
            }

            log.info("Deleting output directory since we have finished the pushes " + outputDir);
            HadoopUtils.deletePathIfExists(jobConf, outputDir.toString());
            log.info("Successfully deleted output directory since we have finished the pushes"
                     + outputDir);
        }

        // ====== Time to swap the stores one node at a time ========
        try {
            for(int index = 0; index < clusterUrls.size(); index++) {
                String url = clusterUrls.get(index);
                Cluster cluster = urlToCluster.get(url);

                AdminClient adminClient = new AdminClient(cluster,
                                                          new AdminClientConfig(),
                                                          new ClientConfig());

                log.info("Swapping all stores on cluster " + url);
                try {
                    // Go over every node and swap
                    for(Node node: cluster.getNodes()) {

                        log.info("Swapping all stores on cluster " + url + " and node "
                                 + node.getId());

                        // Go over every store and swap
                        for(String storeName: storeNames) {

                            Pair<String, String> key = Pair.create(url, storeName);
                            log.info("Swapping '" + storeName + "' store on cluster " + url
                                     + " and node " + node.getId() + " - "
                                     + nodeDirPerClusterStore.get(key).get(node.getId()));

                            previousNodeDirPerClusterStore.put(key,
                                                               Pair.create(node.getId(),
                                                                           adminClient.readonlyOps.swapStore(node.getId(),
                                                                                                             storeName,
                                                                                                             nodeDirPerClusterStore.get(key)
                                                                                                                                   .get(node.getId()))));
                            log.info("Successfully swapped '" + storeName + "' store on cluster "
                                     + url + " and node " + node.getId());

                        }

                    }
                } finally {
                    if(adminClient != null) {
                        adminClient.close();
                    }
                }
            }
        } catch(Exception e) {

            log.error("Got an exception during swaps. Rolling back data already pushed on successful nodes");

            for(Pair<String, String> clusterStoreTuple: previousNodeDirPerClusterStore.keySet()) {
                Collection<Pair<Integer, String>> nodeToPreviousDirs = previousNodeDirPerClusterStore.get(clusterStoreTuple);
                String url = clusterStoreTuple.getFirst();
                Cluster cluster = urlToCluster.get(url);

                log.info("Rolling back for cluster " + url + " and store  "
                         + clusterStoreTuple.getSecond());

                AdminClient adminClient = new AdminClient(cluster,
                                                          new AdminClientConfig(),
                                                          new ClientConfig());
                try {
                    for(Pair<Integer, String> nodeToPreviousDir: nodeToPreviousDirs) {
                        log.info("Rolling back for cluster " + url + " and store "
                                 + clusterStoreTuple.getSecond() + " and node "
                                 + nodeToPreviousDir.getFirst() + " to dir "
                                 + nodeToPreviousDir.getSecond());
                        adminClient.readonlyOps.rollbackStore(nodeToPreviousDir.getFirst(),
                                                              nodeToPreviousDir.getSecond(),
                                                              ReadOnlyUtils.getVersionId(new File(nodeToPreviousDir.getSecond())));
                        log.info("Successfully rolled back for cluster " + url + " and store "
                                 + clusterStoreTuple.getSecond() + " and node "
                                 + nodeToPreviousDir.getFirst() + " to dir "
                                 + nodeToPreviousDir.getSecond());

                    }
                } finally {
                    if(adminClient != null) {
                        adminClient.close();
                    }
                }
            }
            throw e;
        }
    }

    /**
     * Verify if the store exists on the cluster ( pointed by url ). Also use
     * the input path to retrieve the metadata
     * 
     * @param storeName Store name
     * @param url The url of the cluster
     * @param inputPath The input path where the files exist. This will be used
     *        for building the store
     * @param adminClient Admin Client used to verify the schema
     * @return Returns a pair of store definition + cluster metadata
     * @throws IOException Exception due to input path being bad
     */
    public Pair<StoreDefinition, Cluster> verifySchema(String storeName,
                                                       String url,
                                                       Path inputPath,
                                                       AdminClient adminClient) throws IOException {
        // create new json store def with schema from the metadata in the input
        // path
        JsonSchema schema = HadoopUtils.getSchemaFromPath(inputPath);
        int replicationFactor = props.getInt("build.replication.factor." + storeName,
                                             props.getInt("build.replication.factor", 2));
        int requiredReads = props.getInt("build.required.reads." + storeName,
                                         props.getInt("build.required.reads", 1));
        int requiredWrites = props.getInt("build.required.writes." + storeName,
                                          props.getInt("build.required.writes", 1));

        int preferredReads = props.getInt("build.preferred.reads." + storeName,
                                          props.getInt("build.preferred.reads", -1));
        int preferredWrites = props.getInt("build.preferred.writes." + storeName,
                                           props.getInt("build.preferred.writes", -1));

        String description = props.getString("push.store.description." + storeName,
                                             props.getString("push.store.description", ""));
        String owners = props.getString("push.store.owners." + storeName,
                                        props.getString("push.store.owners", ""));

        // Generate the key and value schema
        String keySchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getKeyType() + "</schema-info>\n\t";
        String valSchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getValueType() + "</schema-info>\n\t";

        String keySchemaCompression = "";
        if(props.containsKey("build.compress.key." + storeName)
           || (storeNames.size() == 1 && props.containsKey("build.compress.key"))) {
            keySchemaCompression = "\t<compression><type>gzip</type></compression>\n\t";
            keySchema += keySchemaCompression;
        }

        String valueSchemaCompression = "";
        if(props.containsKey("build.compress.value." + storeName)
           || (storeNames.size() == 1 && props.containsKey("build.compress.value"))) {
            valueSchemaCompression = "\t<compression><type>gzip</type></compression>\n\t";
            valSchema += valueSchemaCompression;
        }

        if(props.containsKey("build.force.schema.key." + storeName)) {
            keySchema = props.get("build.force.schema.key." + storeName);
        }

        if(props.containsKey("build.force.schema.value." + storeName)) {
            valSchema = props.get("build.force.schema.value." + storeName);
        }

        // For backwards compatibility check build.force.schema.*
        if(props.containsKey("build.force.schema.key") && storeNames.size() == 1) {
            keySchema = props.get("build.force.schema.key");
        }

        if(props.containsKey("build.force.schema.value") && storeNames.size() == 1) {
            valSchema = props.get("build.force.schema.value");
        }

        String newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                              replicationFactor,
                                                              requiredReads,
                                                              requiredWrites,
                                                              (preferredReads < 0) ? null
                                                                                  : preferredReads,
                                                              (preferredWrites < 0) ? null
                                                                                   : preferredWrites,
                                                              keySchema,
                                                              valSchema,
                                                              description,
                                                              owners);

        log.info("Verifying store: \n" + newStoreDefXml.toString());

        StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

        // get store def from cluster
        log.info("Getting store definition from: " + url + " ( node id " + this.nodeId + " )");

        List<StoreDefinition> remoteStoreDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(this.nodeId)
                                                                           .getValue();
        boolean foundStore = false;

        // go over all store defs and see if one has the same name as the store
        // we're trying
        // to build
        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(storeName)) {
                // if the store already exists, but doesn't match what we want
                // to push, we need
                // to worry
                if(!remoteStoreDef.equals(newStoreDef)) {
                    // it is possible that the stores actually DO match, but the
                    // json in the key/value serializers is out of order (eg
                    // {'a': 'int32', 'b': 'int32'} could have a/b reversed.
                    // this is just a reflection of the fact that voldemort json
                    // type defs use hashmaps that are unordered, and pig uses
                    // bags that are unordered as well. it's therefore
                    // unpredictable what order the keys will come out of pig.
                    // let's check to see if the key/value serializers are
                    // REALLY equal.
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
                            // if the key/value serializers are REALLY equal
                            // (even though the strings may not match), then
                            // just use the remote stores to GUARANTEE that they
                            // match, and try again.
                            keySchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                                        + remoteKeySerializerDef.getCurrentSchemaInfo()
                                        + "</schema-info>\n\t" + keySchemaCompression;
                            valSchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                                        + remoteValueSerializerDef.getCurrentSchemaInfo()
                                        + "</schema-info>\n\t" + valueSchemaCompression;
                            newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                                           replicationFactor,
                                                                           requiredReads,
                                                                           requiredWrites,
                                                                           (preferredReads < 0) ? null
                                                                                               : preferredReads,
                                                                           (preferredWrites < 0) ? null
                                                                                                : preferredWrites,
                                                                           keySchema,
                                                                           valSchema,
                                                                           description,
                                                                           owners);

                            newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

                            if(!remoteStoreDef.equals(newStoreDef)) {
                                // if we still get a fail, then we know that the
                                // store defs don't match for reasons OTHER than
                                // the key/value serializer
                                throw new RuntimeException("Your store schema is identical, but the store definition does not match. Have: "
                                                           + newStoreDef
                                                           + "\nBut expected: "
                                                           + remoteStoreDef);
                            }
                        } else {
                            // if the key/value serializers are not equal (even
                            // in java, not just json strings), then fail
                            throw new RuntimeException("Your store definition does not match the store definition that is already in the cluster. Tried to resolve identical schemas between local and remote, but failed. Have: "
                                                       + newStoreDef
                                                       + "\nBut expected: "
                                                       + remoteStoreDef);
                        }
                    }
                }

                foundStore = true;
                break;
            }
        }

        // if the store doesn't exist yet, create it
        if(!foundStore) {
            // New requirement - Make sure the user had description and owner
            // specified
            if(description.length() == 0) {
                throw new RuntimeException("Description field missing in store definition. "
                                           + "Please add \"push.store.description\" with a line describing your store");
            }

            if(owners.length() == 0) {
                throw new RuntimeException("Owner field missing in store definition. "
                                           + "Please add \"push.store.owners\" with value being comma-separated list of LinkedIn email ids");

            }

            log.info("Could not find store " + storeName
                     + " on Voldemort. Adding it to all nodes for cluster " + url);
            adminClient.storeMgmtOps.addStore(newStoreDef);
        }

        // don't use newStoreDef because we want to ALWAYS use the JSON
        // definition since the store builder assumes that you are using
        // JsonTypeSerializer. This allows you to tweak your value/key store xml
        // as you see fit, but still uses the json sequence file meta data to
        // build the store.
        StoreDefinition storeDef = VoldemortUtils.getStoreDef(VoldemortUtils.getStoreDefXml(storeName,
                                                                                            replicationFactor,
                                                                                            requiredReads,
                                                                                            requiredWrites,
                                                                                            (preferredReads < 0) ? null
                                                                                                                : preferredReads,
                                                                                            (preferredWrites < 0) ? null
                                                                                                                 : preferredWrites,
                                                                                            keySchema,
                                                                                            valSchema,
                                                                                            description,
                                                                                            owners));
        Cluster cluster = adminClient.getAdminClientCluster();

        return Pair.create(storeDef, cluster);
    }

    /**
     * Only build the store
     * 
     * @param cluster Cluster metadata
     * @param storeDef Store definition metadata
     * @param inputPath The input path where the current data is present
     * @param outputPath The output location where we'll like to store our data
     * @throws Exception
     */
    public void runBuildStore(Cluster cluster,
                              StoreDefinition storeDef,
                              Path inputPath,
                              Path outputPath) throws Exception {
        int chunkSize = props.getInt("build.chunk.size." + storeDef.getName(),
                                     props.getInt("build.chunk.size", (int) 1024 * 1024 * 1024));
        Path tempDir = new Path(props.getString("build.temp.dir", "/tmp/voldemort-build-temp-"
                                                                  + new Random().nextLong()));

        String keySelection = props.getString("build.key.selection." + storeDef.getName(),
                                              props.getString("build.key.selection", null));
        String valSelection = props.getString("build.value.selection." + storeDef.getName(),
                                              props.getString("build.key.selection", null));

        int numChunks = props.getInt("num.chunks." + storeDef.getName(),
                                     props.getInt("num.chunks", -1));

        CheckSumType checkSumType = CheckSum.fromString(props.getString("checksum.type",
                                                                        CheckSum.toString(CheckSumType.MD5)));
        boolean saveKeys = props.getBoolean("save.keys", true);
        boolean reducerPerBucket = props.getBoolean("reducer.per.bucket", false);

        new VoldemortStoreBuilderJob(this.getId() + "-build-store",
                                     props,
                                     new VoldemortStoreBuilderConf(storeDef.getReplicationFactor(),
                                                                   chunkSize,
                                                                   tempDir,
                                                                   outputPath,
                                                                   inputPath,
                                                                   cluster,
                                                                   Lists.newArrayList(storeDef),
                                                                   storeDef.getName(),
                                                                   keySelection,
                                                                   valSelection,
                                                                   null,
                                                                   null,
                                                                   checkSumType,
                                                                   saveKeys,
                                                                   reducerPerBucket,
                                                                   numChunks)).run();
    }

}

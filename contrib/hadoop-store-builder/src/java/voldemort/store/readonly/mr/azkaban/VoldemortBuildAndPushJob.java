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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.azkaban.VoldemortStoreBuilderJob.VoldemortStoreBuilderConf;
import voldemort.store.readonly.mr.azkaban.VoldemortSwapJob.VoldemortSwapConf;
import voldemort.store.readonly.mr.utils.AvroUtils;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.JsonSchema;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.utils.Utils;
import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class VoldemortBuildAndPushJob extends AbstractJob {

    private final Logger log;

    private final Props props;

    private Cluster cluster;

    private List<StoreDefinition> storeDefs;

    private final String storeName;

    private final List<String> clusterUrl;

    private final int nodeId;

    private final List<String> dataDirs;

    // Reads from properties to check if this takes Avro input
    private final boolean isAvroJob;

    private final String keyField;

    private final String valueField;

    private final boolean isAvroVersioned;

    private static final String AVRO_GENERIC_TYPE_NAME = "avro-generic";

    // New serialization types for avro versioning support
    // We cannot change existing serializer classes since
    // this will break existing clients while looking for the version byte

    private static final String AVRO_GENERIC_VERSIONED_TYPE_NAME = "avro-generic-versioned";

    // new properties for the push job

    private final String hdfsFetcherPort;
    private final String hdfsFetcherProtocol;

    /* Informed stuff */
    private final String informedURL = "http://informed.corp.linkedin.com/_post";
    private final List<Future> informedResults;
    private ExecutorService informedExecutor;

    private String jsonKeyField;
    private String jsonValueField;

    public VoldemortBuildAndPushJob(String name, Props props) {
        super(name);
        this.props = props;
        this.storeName = props.getString("push.store.name").trim();
        this.clusterUrl = new ArrayList<String>();
        this.dataDirs = new ArrayList<String>();

        String clusterUrlText = props.getString("push.cluster");
        for(String url: Utils.COMMA_SEP.split(clusterUrlText.trim()))
            if(url.trim().length() > 0)
                this.clusterUrl.add(url);

        if(clusterUrl.size() <= 0)
            throw new RuntimeException("Number of urls should be atleast 1");

        // Support multiple output dirs if the user mentions only "push", no
        // "build".
        // If user mentions both then should have only one
        String dataDirText = props.getString("build.output.dir");
        for(String dataDir: Utils.COMMA_SEP.split(dataDirText.trim()))
            if(dataDir.trim().length() > 0)
                this.dataDirs.add(dataDir);

        if(dataDirs.size() <= 0)
            throw new RuntimeException("Number of data dirs should be atleast 1");

        this.nodeId = props.getInt("push.node", 0);
        this.log = Logger.getLogger(name);
        this.informedResults = Lists.newArrayList();
        this.informedExecutor = Executors.newFixedThreadPool(2);

        this.hdfsFetcherProtocol = props.getString("voldemort.fetcher.protocol", "hftp");
        this.hdfsFetcherPort = props.getString("voldemort.fetcher.port", "50070");

        log.info("voldemort.fetcher.protocol is set to : " + hdfsFetcherProtocol);
        log.info("voldemort.fetcher.port is set to : " + hdfsFetcherPort);

        isAvroJob = props.getBoolean("build.type.avro", false);

        // Set default to false
        // this ensures existing clients who are not aware of the new serializer
        // type dont bail out
        isAvroVersioned = props.getBoolean("avro.serializer.versioned", false);

        keyField = props.getString("avro.key.field", null);

        valueField = props.getString("avro.value.field", null);

        if(isAvroJob) {
            if(keyField == null)
                throw new RuntimeException("The key field must be specified in the properties for the Avro build and push job!");

            if(valueField == null)
                throw new RuntimeException("The value field must be specified in the properties for the Avro build and push job!");

        }

    }

    @Override
    public void run() throws Exception {
        boolean build = props.getBoolean("build", true);
        boolean push = props.getBoolean("push", true);

        jsonKeyField = props.getString("key.selection", null);
        jsonValueField = props.getString("value.selection", null);
        if(build && push && dataDirs.size() != 1) {
            // Should have only one data directory ( which acts like the parent
            // directory to all
            // urls )
            throw new RuntimeException(" Should have only one data directory ( which acts like root directory ) since they are auto-generated during build phase ");
        } else if(!build && push && dataDirs.size() != clusterUrl.size()) {
            // Number of data directories should be equal to number of cluster
            // urls
            throw new RuntimeException(" Since we are only pushing, number of data directories ( comma separated ) should be equal to number of cluster urls ");
        }

        // Check every url individually
        HashMap<String, Exception> exceptions = Maps.newHashMap();

        for(int index = 0; index < clusterUrl.size(); index++) {
            String url = clusterUrl.get(index);

            log.info("Working on " + url);

            try {

                if(isAvroJob)
                    verifyAvroSchemaAndVersions(url, isAvroVersioned);
                else
                    verifySchema(url);

                String buildOutputDir;
                if(build) {
                    buildOutputDir = runBuildStore(props, url);
                } else {
                    buildOutputDir = dataDirs.get(index);
                }

                if(push) {
                    if(log.isDebugEnabled())
                        log.debug("Informing about push start ...");
                    informedResults.add(this.informedExecutor.submit(new InformedClient(this.props,
                                                                                        "Running",
                                                                                        this.getId())));

                    runPushStore(props, url, buildOutputDir);
                }

                if(build && push && !props.getBoolean("build.output.keep", false)) {
                    JobConf jobConf = new JobConf();

                    if(props.containsKey("hadoop.job.ugi")) {
                        jobConf.set("hadoop.job.ugi", props.getString("hadoop.job.ugi"));
                    }

                    log.info("Deleting " + buildOutputDir);
                    HadoopUtils.deletePathIfExists(jobConf, buildOutputDir);
                    log.info("Deleted " + buildOutputDir);
                }

                if(log.isDebugEnabled())
                    log.debug("Informing about push finish ...");
                informedResults.add(this.informedExecutor.submit(new InformedClient(this.props,
                                                                                    "Finished",
                                                                                    this.getId())));

                for(Future result: informedResults) {
                    try {
                        result.get();
                    } catch(Exception e) {
                        this.log.error("Exception in consumer", e);
                    }
                }
                this.informedExecutor.shutdownNow();
            } catch(Exception e) {
                log.error("Exception during build and push for url " + url, e);
                exceptions.put(url, e);
            }
        }

        if(exceptions.size() > 0) {
            log.error("Got exceptions while pushing to " + Joiner.on(",").join(exceptions.keySet())
                      + " => " + Joiner.on(",").join(exceptions.values()));
            System.exit(-1);
        }
    }

    public void verifySchema(String url) throws Exception {
        // create new json store def with schema from the metadata in the input
        // path
        JsonSchema schema = HadoopUtils.getSchemaFromPath(getInputPath());
        int replicationFactor = props.getInt("build.replication.factor", 2);
        int requiredReads = props.getInt("build.required.reads", 1);
        int requiredWrites = props.getInt("build.required.writes", 1);
        String description = props.getString("push.store.description", "");
        String owners = props.getString("push.store.owners", "");
        String keySchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getKeyType() + "</schema-info>\n\t";
        if(jsonKeyField != null && jsonKeyField.length() > 0)
            keySchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                        + schema.getKeyType().subtype(jsonKeyField) + "</schema-info>\n\t";
        String valSchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getValueType() + "</schema-info>\n\t";

        if(jsonValueField != null && jsonValueField.length() > 0)
            valSchema = "\n\t\t<type>json</type>\n\t\t<schema-info version=\"0\">"
                        + schema.getValueType().subtype(jsonValueField) + "</schema-info>\n\t";

        boolean hasCompression = false;
        if(props.containsKey("build.compress.value"))
            hasCompression = true;

        if(hasCompression) {
            valSchema += "\t<compression><type>gzip</type></compression>\n\t";
        }

        if(props.containsKey("build.force.schema.key")) {
            keySchema = props.get("build.force.schema.key");
        }

        if(props.containsKey("build.force.schema.value")) {
            valSchema = props.get("build.force.schema.value");
        }

        String newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                              replicationFactor,
                                                              requiredReads,
                                                              requiredWrites,
                                                              props.containsKey("build.preferred.reads") ? props.getInt("build.preferred.reads")
                                                                                                        : null,
                                                              props.containsKey("build.preferred.writes") ? props.getInt("build.preferred.writes")
                                                                                                         : null,
                                                              (props.containsKey("push.force.schema.key")) ? props.getString("push.force.schema.key")
                                                                                                          : keySchema,
                                                              (props.containsKey("push.force.schema.value")) ? props.getString("push.force.schema.value")
                                                                                                            : valSchema,
                                                              description,
                                                              owners);

        log.info("Verifying store: \n" + newStoreDefXml.toString());

        StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

        // get store def from cluster
        log.info("Getting store definition from: " + url + " (node id " + this.nodeId + ")");

        AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig());
        try {
            List<StoreDefinition> remoteStoreDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(this.nodeId)
                                                                               .getValue();
            boolean foundStore = false;

            // go over all store defs and see if one has the same name as the
            // store we're trying
            // to build
            for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
                if(remoteStoreDef.getName().equals(storeName)) {
                    // if the store already exists, but doesn't match what we
                    // want to push, we need
                    // to worry
                    if(!remoteStoreDef.equals(newStoreDef)) {
                        // it is possible that the stores actually DO match, but
                        // the
                        // json in the key/value serializers is out of order (eg
                        // {'a': 'int32', 'b': 'int32'} could have a/b reversed.
                        // this is just a reflection of the fact that voldemort
                        // json
                        // type defs use hashmaps that are unordered, and pig
                        // uses
                        // bags that are unordered as well. it's therefore
                        // unpredictable what order the keys will come out of
                        // pig.
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
                                String compressionPolicy = "";
                                if(hasCompression) {
                                    compressionPolicy = "\n\t\t<compression><type>gzip</type></compression>";
                                }

                                // if the key/value serializers are REALLY equal
                                // (even though the strings may not match), then
                                // just use the remote stores to GUARANTEE that
                                // they
                                // match, and try again.
                                newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                                               replicationFactor,
                                                                               requiredReads,
                                                                               requiredWrites,
                                                                               props.containsKey("build.preferred.reads") ? props.getInt("build.preferred.reads")
                                                                                                                         : null,
                                                                               props.containsKey("build.preferred.writes") ? props.getInt("build.preferred.writes")
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
                                    // if we still get a fail, then we know that
                                    // the
                                    // store defs don't match for reasons OTHER
                                    // than
                                    // the key/value serializer
                                    throw new RuntimeException("Your store schema is identical, but the store definition does not match. Have: "
                                                               + newStoreDef
                                                               + "\nBut expected: "
                                                               + remoteStoreDef);
                                }
                            } else {
                                // if the key/value serializers are not equal
                                // (even
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
                // New requirement - Make sure the user had description and
                // owner specified
                if(description.length() == 0) {
                    throw new RuntimeException("Description field missing in store definition. "
                                               + "Please add \"push.store.description\" with a line describing your store");
                }

                if(owners.length() == 0) {
                    throw new RuntimeException("Owner field missing in store definition. "
                                               + "Please add \"push.store.owners\" with value being comma-separated list of LinkedIn email ids");

                }

                log.info("Could not find store " + storeName
                         + " on Voldemort. Adding it to all nodes ");
                adminClient.storeMgmtOps.addStore(newStoreDef);
            }

            // don't use newStoreDef because we want to ALWAYS use the JSON
            // definition since the store builder assumes that you are using
            // JsonTypeSerializer. This allows you to tweak your value/key store
            // xml
            // as you see fit, but still uses the json sequence file meta data
            // to
            // build the store.
            storeDefs = ImmutableList.of(VoldemortUtils.getStoreDef(VoldemortUtils.getStoreDefXml(storeName,
                                                                                                  replicationFactor,
                                                                                                  requiredReads,
                                                                                                  requiredWrites,
                                                                                                  props.containsKey("build.preferred.reads") ? props.getInt("build.preferred.reads")
                                                                                                                                            : null,
                                                                                                  props.containsKey("build.preferred.writes") ? props.getInt("build.preferred.writes")
                                                                                                                                             : null,
                                                                                                  keySchema,
                                                                                                  valSchema)));
            cluster = adminClient.getAdminClientCluster();
        } finally {
            adminClient.close();
        }
    }

    public String runBuildStore(Props props, String url) throws Exception {
        int replicationFactor = props.getInt("build.replication.factor", 2);
        int chunkSize = props.getInt("build.chunk.size", 1024 * 1024 * 1024);
        Path tempDir = new Path(props.getString("build.temp.dir", "/tmp/vold-build-and-push-"
                                                                  + new Random().nextLong()));
        URI uri = new URI(url);
        Path outputDir = new Path(props.getString("build.output.dir"), uri.getHost());
        Path inputPath = getInputPath();
        String keySelection = props.getString("key.selection", null);
        String valSelection = props.getString("value.selection", null);
        CheckSumType checkSumType = CheckSum.fromString(props.getString("checksum.type",
                                                                        CheckSum.toString(CheckSumType.MD5)));
        boolean saveKeys = props.getBoolean("save.keys", true);
        boolean reducerPerBucket = props.getBoolean("reducer.per.bucket", false);
        int numChunks = props.getInt("num.chunks", -1);

        if(isAvroJob) {
            String recSchema = getRecordSchema();
            String keySchema = getKeySchema();
            String valSchema = getValueSchema();

            new VoldemortStoreBuilderJob(this.getId() + "-build-store",
                                         props,
                                         new VoldemortStoreBuilderConf(replicationFactor,
                                                                       chunkSize,
                                                                       tempDir,
                                                                       outputDir,
                                                                       inputPath,
                                                                       cluster,
                                                                       storeDefs,
                                                                       storeName,
                                                                       keySelection,
                                                                       valSelection,
                                                                       null,
                                                                       null,
                                                                       checkSumType,
                                                                       saveKeys,
                                                                       reducerPerBucket,
                                                                       numChunks,
                                                                       keyField,
                                                                       valueField,
                                                                       recSchema,
                                                                       keySchema,
                                                                       valSchema), true).run();
            return outputDir.toString();
        }
        new VoldemortStoreBuilderJob(this.getId() + "-build-store",
                                     props,
                                     new VoldemortStoreBuilderConf(replicationFactor,
                                                                   chunkSize,
                                                                   tempDir,
                                                                   outputDir,
                                                                   inputPath,
                                                                   cluster,
                                                                   storeDefs,
                                                                   storeName,
                                                                   keySelection,
                                                                   valSelection,
                                                                   null,
                                                                   null,
                                                                   checkSumType,
                                                                   saveKeys,
                                                                   reducerPerBucket,
                                                                   numChunks)).run();
        return outputDir.toString();
    }

    public void runPushStore(Props props, String url, String dataDir) throws Exception {
        // For backwards compatibility http timeout = admin timeout
        int httpTimeoutMs = 1000 * props.getInt("push.http.timeout.seconds", 24 * 60 * 60);
        long pushVersion = props.getLong("push.version", -1L);
        if(props.containsKey("push.version.timestamp")) {
            DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            pushVersion = Long.parseLong(format.format(new Date()));
        }
        int maxBackoffDelayMs = 1000 * props.getInt("push.backoff.delay.seconds", 60);
        boolean rollback = props.getBoolean("push.rollback", true);

        new VoldemortSwapJob(this.getId() + "-push-store",
                             props,
                             new VoldemortSwapConf(cluster,
                                                   dataDir,
                                                   storeName,
                                                   httpTimeoutMs,
                                                   pushVersion,
                                                   maxBackoffDelayMs,
                                                   rollback)).run();
    }

    /**
     * Get the sanitized input path. At the moment of writing, this means the
     * #LATEST tag is expanded.
     */
    private Path getInputPath() throws IOException {
        Path path = new Path(props.getString("build.input.path"));
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

        String keySchema = schema.getField(keyField).schema().toString();

        return keySchema;

    }

    // Extract schema of the value field
    public String getValueSchema() throws IOException {
        Schema schema = AvroUtils.getAvroSchemaFromPath(getInputPath());

        String valueSchema = schema.getField(valueField).schema().toString();

        return valueSchema;

    }

    // Verify if the new avro schema being pushed is the same one as the last
    // version present on the server
    // supports schema evolution

    public void verifyAvroSchemaAndVersions(String url, boolean isVersioned) throws Exception {
        // create new n store def with schema from the metadata in the input
        // path
        Schema schema = AvroUtils.getAvroSchemaFromPath(getInputPath());
        int replicationFactor = props.getInt("build.replication.factor", 2);
        int requiredReads = props.getInt("build.required.reads", 1);
        int requiredWrites = props.getInt("build.required.writes", 1);
        String description = props.getString("push.store.description", "");
        String owners = props.getString("push.store.owners", "");

        String serializerName;

        if(isVersioned)
            serializerName = AVRO_GENERIC_VERSIONED_TYPE_NAME;
        else
            serializerName = AVRO_GENERIC_TYPE_NAME;

        String keySchema = "\n\t\t<type>" + serializerName
                           + "</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getField(keyField).schema() + "</schema-info>\n\t";
        String valSchema = "\n\t\t<type>" + serializerName
                           + "</type>\n\t\t<schema-info version=\"0\">"
                           + schema.getField(valueField).schema() + "</schema-info>\n\t";

        boolean hasCompression = false;
        if(props.containsKey("build.compress.value"))
            hasCompression = true;

        if(hasCompression) {
            valSchema += "\t<compression><type>gzip</type></compression>\n\t";
        }

        if(props.containsKey("build.force.schema.key")) {
            keySchema = props.get("build.force.schema.key");
        }

        if(props.containsKey("build.force.schema.value")) {
            valSchema = props.get("build.force.schema.value");
        }

        String newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                              replicationFactor,
                                                              requiredReads,
                                                              requiredWrites,
                                                              props.containsKey("build.preferred.reads") ? props.getInt("build.preferred.reads")
                                                                                                        : null,
                                                              props.containsKey("build.preferred.writes") ? props.getInt("build.preferred.writes")
                                                                                                         : null,
                                                              (props.containsKey("push.force.schema.key")) ? props.getString("push.force.schema.key")
                                                                                                          : keySchema,
                                                              (props.containsKey("push.force.schema.value")) ? props.getString("push.force.schema.value")
                                                                                                            : valSchema,
                                                              description,
                                                              owners);

        log.info("Verifying store: \n" + newStoreDefXml.toString());

        StoreDefinition newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

        // get store def from cluster
        log.info("Getting store definition from: " + url + " (node id " + this.nodeId + ")");

        AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig());
        try {
            List<StoreDefinition> remoteStoreDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(this.nodeId)
                                                                               .getValue();
            boolean foundStore = false;

            // go over all store defs and see if one has the same name as the
            // store we're trying
            // to build
            for(StoreDefinition remoteStoreDef: remoteStoreDefs) {

                if(remoteStoreDef.getName().equals(storeName)) {
                    // if the store already exists, but doesn't match what we
                    // want to push, we need
                    // to worry

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

                                keySchema = keySerializerStr;
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
                                valSchema = valueSerializerStr;

                                newStoreDefXml = VoldemortUtils.getStoreDefXml(storeName,
                                                                               replicationFactor,
                                                                               requiredReads,
                                                                               requiredWrites,
                                                                               props.containsKey("build.preferred.reads") ? props.getInt("build.preferred.reads")
                                                                                                                         : null,
                                                                               props.containsKey("build.preferred.writes") ? props.getInt("build.preferred.writes")
                                                                                                                          : null,
                                                                               keySerializerStr,
                                                                               valueSerializerStr);

                                newStoreDef = VoldemortUtils.getStoreDef(newStoreDefXml);

                                if(!remoteStoreDef.equals(newStoreDef)) {
                                    // if we still get a fail, then we know that
                                    // the
                                    // store defs don't match for reasons OTHER
                                    // than
                                    // the key/value serializer
                                    throw new RuntimeException("Your store schema is identical, but the store definition does not match. Have: "
                                                               + newStoreDef
                                                               + "\nBut expected: "
                                                               + remoteStoreDef);
                                }

                            } else {
                                // if the key/value serializers are not equal
                                // (even
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

                // New requirement - Make sure the user had description and
                // owner specified
                if(description.length() == 0) {
                    throw new RuntimeException("Description field missing in store definition. "
                                               + "Please add \"push.store.description\" with a line describing your store");
                }

                if(owners.length() == 0) {
                    throw new RuntimeException("Owner field missing in store definition. "
                                               + "Please add \"push.store.owners\" with value being comma-separated list of LinkedIn email ids");

                }

                log.info("Could not find store " + storeName
                         + " on Voldemort. Adding it to all nodes ");
                adminClient.storeMgmtOps.addStore(newStoreDef);
            }

            storeDefs = ImmutableList.of(VoldemortUtils.getStoreDef(VoldemortUtils.getStoreDefXml(storeName,
                                                                                                  replicationFactor,
                                                                                                  requiredReads,
                                                                                                  requiredWrites,
                                                                                                  props.containsKey("build.preferred.reads") ? props.getInt("build.preferred.reads")
                                                                                                                                            : null,
                                                                                                  props.containsKey("build.preferred.writes") ? props.getInt("build.preferred.writes")
                                                                                                                                             : null,
                                                                                                  keySchema,
                                                                                                  valSchema)));
            cluster = adminClient.getAdminClientCluster();
        } finally {
            adminClient.close();
        }
    }

    private class InformedClient implements Runnable {

        private Props props;
        private String status;
        private String source;

        public InformedClient(Props props, String status, String source) {
            this.props = props;
            this.status = status;
            this.source = source;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            try {
                URL url = new URL(informedURL);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setDoInput(true);
                conn.setRequestProperty("Content-Type", "application/json");

                String storeName = this.props.getString("push.store.name", "null");
                String clusterName = this.props.getString("push.cluster", "null");
                String owners = this.props.getString("push.store.owners", "null");
                String replicationFactor = this.props.getString("build.replication.factor", "null");

                // JSON Object did not work for some reason. Hence doing my own
                // Json.
                String message = "Store : " + storeName.replaceAll("[\'\"]", "") + ",  Status : "
                                 + this.status.replaceAll("[\'\"]", "") + ",  URL : "
                                 + clusterName.replaceAll("[\'\"]", "") + ",  owners : "
                                 + owners.replaceAll("[\'\"]", "") + ",  replication : "
                                 + replicationFactor.replaceAll("[\'\"]", "");
                String payloadStr = "{\"message\":\"" + message
                                    + "\",\"topic\":\"build-and-push\",\"source\":\"" + this.source
                                    + "\",\"user\":\"bandp\"}";
                if(log.isDebugEnabled())
                    log.debug("Payload : " + payloadStr);

                OutputStream out = conn.getOutputStream();
                out.write(payloadStr.getBytes());
                out.close();

                if(conn.getResponseCode() != 200) {
                    System.out.println(conn.getResponseCode());
                    log.error("Illegal response : " + conn.getResponseMessage());
                    throw new IOException(conn.getResponseMessage());
                }

                // Buffer the result into a string
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;
                while((line = rd.readLine()) != null) {
                    sb.append(line);
                }
                rd.close();

                if(log.isDebugEnabled())
                    log.debug("Received response: " + sb);

                conn.disconnect();

            } catch(Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }

    }

}

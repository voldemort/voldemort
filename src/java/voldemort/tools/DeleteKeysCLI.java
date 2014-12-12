/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import voldemort.VoldemortClientShell;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * This tool reads from a keyfile and deletes
 *   the key from the supplied stores. The tools are considered to be in
 *   human readable format and conversion will be attempted to the
 *   appropriate key.
 *
 *   First of all understand that
 *
 *   The tool also supports the following options
 *     1) --delete-all-versions. If you have more than one value with
 *     conflicting versions, the tool will fail, because it may not have the
 *   value schema to de-serialize the value and resolve the conflict. The
 *   conflict resolution needs to happen before the key is deleted.
 *     2) --nodeid <> --admin-url <>. If you want to delete keys only from a
 *     particular node. Use the above options. It is useful when you delete the
 *   keys and if a node went down, you want to rerun the tool with that
 *   option.
 *     3) --find-keys-exist <> . After the delete you can run with this
 *     option to find if any of the keys exist. If the keys are found the tool
 *   dumps the version of each of the keys. The tool waits for the number of
 *   keys from each store before it completes.
 *
 *   The tool creates the following files in the same directory as it is being run
 *   <storename>_errors.txt - This contain the key and the exception and additional comments
 *   <storename>_failure.txt - This contain just the failed keys. You can rename this file and
 *       pass it as input for the next run. if you dont rename it the file will get overwritten
 *       the next time which will become harder to debug.
 *   <storename>_success.txt - The keys for which the call succeeded. It includes the missing keys.
 *   <storename>_missing.txt - The keys which are not found in the store.
 *   <storename>_find_keys.txt - If invoked with find-keys-exist option will dump the keys and versions
 *   <storename>_skipped.txt - if invoked with nodeid option will dump the skipped keys.
 *
 *   The tool creates one single file called status.txt
 *   The status.txt periodically dumps some statistics about each of the stores, this may or may not be
 *   interesting to you.
 */

public class DeleteKeysCLI {

    private static final Logger logger = Logger.getLogger(ZoneClipperCLI.class);

    private static OptionParser setupParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "Print usage information").withOptionalArg();
        parser.accepts("url", "bootstrapUrl").withRequiredArg().describedAs("bootstrap url");
        parser.accepts("zone", "Zone id")
              .withRequiredArg()
              .describedAs("zone id")
              .ofType(Integer.class)
              .defaultsTo(-1);
        parser.accepts("stores", "store")
              .withRequiredArg()
              .describedAs("stores to delete the key/value from")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("keyfile", "key file")
              .withRequiredArg()
              .describedAs("file with keys to be deleted are stored as one per line");
        parser.accepts("qps", "keys to be deleted per store")
              .withRequiredArg()
              .describedAs("number of operations allowed per second per store")
              .ofType(Integer.class)
              .defaultsTo(100);
        parser.accepts("delete-all-versions", "Deletes all versions for a given key")
              .withOptionalArg();
        parser.accepts("nodeid", "Delete keys if it is hosted in a node")
              .withRequiredArg()
              .describedAs("Delete keys belonging to a node")
              .ofType(Integer.class)
              .defaultsTo(-1);
        parser.accepts("admin-url", "admin url").withRequiredArg().describedAs("admin url");
        parser.accepts("check-keys-exist", "Verify if the number of keys exist")
              .withRequiredArg()
              .describedAs("Check if the given number of keys exist in the store")
              .ofType(Integer.class)
              .defaultsTo(100);
        return parser;
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("DeleteKeysCLI\n");
        help.append("  Deletes record with the given key in the keyfile from the supplied stores\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --url <bootstrapUrl>\n");
        help.append("    --stores <Stores comma seperated>\n");
        help.append("    --keyfile <Path Of teh KeyFile> \n");
        help.append("  Optional:\n");
        help.append("    --qps [ max number of records allowed to be deleted per second per store ]\n");
        help.append("    --delete-all-versions [ Delete all versions when more than one version is found, useful if you dont have deserializer as normal delete will fail ]\n");
        help.append("    --nodeid [ If you want to delete keys that belongs only to a particular node ]\n");
        help.append("    --admin-url [ admin boot strap URL, required when the nodeid parameter is passed ]\n");
        help.append("    --check-keys-exist [ Check if the given number of keys exist in the store ]\n");
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    protected final SocketStoreClientFactory factory;
    protected final List<String> stores;
    protected final Map<String, StoreClient<Object, Object>> storeClients;
    protected final Map<String, SerializerDefinition> serializerDefs;
    protected final AdminClient adminClient;
    protected final int qps;
    protected final String keyFile;
    protected final int nodeid;
    protected final boolean deleteAllVersions;
    protected final int checkKeysCount;

    public DeleteKeysCLI(String url,
                         String adminUrl,
                         List<String> stores,
                         String keyFile,
                         int zoneId,
                         int qps,
                         int nodeid,
                         boolean deleteAllVersions,
                         int checkKeysCount) throws Exception {

        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(url)
                                                      .setEnableLazy(false)
                                                      .setRequestFormatType(RequestFormatType.VOLDEMORT_V3)
                                                      .setMaxConnectionsPerNode(15);

        if(zoneId != Zone.DEFAULT_ZONE_ID) {
            clientConfig.setClientZoneId(zoneId);
        }

        this.stores = stores;

        if(nodeid != -1) {
            if(adminUrl == null || adminUrl.length() == 0) {
                throw new Exception("When deleting from a specific node admin URL is expected");
            }
            adminClient = new AdminClient(adminUrl, new AdminClientConfig(), clientConfig);
        } else {
            adminClient = null;
        }
        factory = new SocketStoreClientFactory(clientConfig);
        storeClients = new HashMap<String, StoreClient<Object, Object>>();
        serializerDefs = new HashMap<String, SerializerDefinition>();
        for(String store: stores) {
            storeClients.put(store, factory.getStoreClient(store));
            StoreDefinition storeDef = StoreUtils.getStoreDef(factory.getStoreDefs(), store);
            serializerDefs.put(store, storeDef.getKeySerializer());
        }

        this.qps = qps;
        this.nodeid = nodeid;
        this.deleteAllVersions = deleteAllVersions;
        this.keyFile = keyFile;
        this.checkKeysCount = checkKeysCount;
    }

    public static class DeleteKeysTask implements Runnable {

        private List<Version> getAllVersions(Object key) throws Exception {
            DefaultStoreClient<Object, Object> defaultStoreClient = (DefaultStoreClient<Object, Object>) client;
            List<Version> returnValue = (List<Version>) getVersionsMethod.invoke(defaultStoreClient,
                                                                                 key);

            return returnValue;
        }

        protected final BufferedWriter errorWriter;
        protected final BufferedWriter successWriter;
        protected final BufferedWriter failedKeyWriter;
        protected final BufferedWriter missingKeyWriter;
        protected final BufferedWriter skipKeyWriter;
        protected final BufferedWriter findKeyWriter;
        protected final List<BufferedWriter> writers;
        protected final BufferedReader keyFileReader;
        protected boolean isComplete = false;
        protected final StoreClient<Object, Object> client;
        protected final AdminClient adminClient;
        protected final SerializerDefinition serializerDef;
        protected final String store;
        protected final int qps;
        protected final boolean deleteAllVersions;
        protected final PrintStream parseErrorStream;
        protected final int nodeid;
        protected final int checkKeysCount;
        private final Method getVersionsMethod;
        private final Serializer<Object> keySerializer;
        final static long millisInSeconds = TimeUnit.SECONDS.toMillis(1);
        long keysProcessed = 0;
        long totalProcessed = 0;
        long versionCallsMade = 0;
        long totalVersions = 0;

        // check Keys statistics
        long totalKeysFound = 0;
        long minTimeStamp = 0;
        long maxTimeStamp = 0;
        boolean isTimeStampInitialized = false;

        private void updateKeyCounters(Version v) {
            if(!(v instanceof VectorClock))
                throw new IllegalArgumentException("Cannot compare Versions of different types.");

            VectorClock vClock = (VectorClock) v;
            long timeStamp = vClock.getTimestamp();
            if(isTimeStampInitialized == false) {
                isTimeStampInitialized = true;
                minTimeStamp = timeStamp;
                maxTimeStamp = timeStamp;
            } else {
                minTimeStamp = Math.min(minTimeStamp, timeStamp);
                maxTimeStamp = Math.max(maxTimeStamp, timeStamp);
            }
        }

        public boolean isComplete() {
            return isComplete;
        }

        public String getStatus() {
            String message = "Store " + store + " processed Keys " + keysProcessed
                             + " actions processed " + totalProcessed;
            if(versionCallsMade > 0 || totalVersions > 0) {
                message += " Version Calls made " + versionCallsMade + " total versions returned "
                           + totalVersions;
            }

            if(this.totalKeysFound > 0) {
                message += " Found Keys " + this.totalKeysFound;
            }

            if(isTimeStampInitialized) {
                message += " Min TimeStamp " + new Date(minTimeStamp) + " Max Timestamp "
                           + new Date(maxTimeStamp);
            }
            if(isComplete) {
                message += " ... completed";
            }
            return message + "\n";
        }

        private boolean shouldProcessKey(Object key) {
            if(nodeid == -1) {
                return true;
            }
            List<Node> nodes = client.getResponsibleNodes(key);
            for(Node node: nodes) {
                if(node.getId() == nodeid) {
                    return true;
                }
            }
            return false;
        }

        public DeleteKeysTask(String store,
                              StoreClient<Object, Object> client,
                              AdminClient adminClient,
                              SerializerDefinition serializerDef,
                              String keyFile,
                              int qps,
                              int nodeid,
                              boolean deleteAllVersions,
                              int checkKeysCount) throws Exception {
            this.store = store;
            this.client = client;
            this.serializerDef = serializerDef;
            this.keyFileReader = new BufferedReader(new FileReader(keyFile));
            this.qps = qps;
            this.nodeid = nodeid;
            this.adminClient = adminClient;
            this.deleteAllVersions = deleteAllVersions;
            this.checkKeysCount = checkKeysCount;

            DefaultStoreClient<Object, Object> defClient = (DefaultStoreClient<Object, Object>) client;

            this.getVersionsMethod = DefaultStoreClient.class.getDeclaredMethod("getVersions",
                                                                                Object.class);

            getVersionsMethod.setAccessible(true);

            this.errorWriter = new BufferedWriter(new FileWriter(store + "_errors.txt", true));
            this.successWriter = new BufferedWriter(new FileWriter(store + "_success.txt", true));
            this.failedKeyWriter = new BufferedWriter(new FileWriter(store + "_failure.txt", true));
            this.missingKeyWriter = new BufferedWriter(new FileWriter(store + "_missing.txt", true));

            writers = new ArrayList<BufferedWriter>(Arrays.asList(errorWriter,
                                                                  successWriter,
                                                                  failedKeyWriter,
                                                                  missingKeyWriter));
            if(this.nodeid != -1) {
                this.skipKeyWriter = new BufferedWriter(new FileWriter(store + "_skipped.txt", true));
                writers.add(skipKeyWriter);
                SerializerFactory serializerFactory = new DefaultSerializerFactory();
                this.keySerializer = (Serializer<Object>) serializerFactory.getSerializer(serializerDef);
            } else {
                this.skipKeyWriter = null;
                this.keySerializer = null;
            }

            if(this.checkKeysCount > 0) {
                findKeyWriter = new BufferedWriter(new FileWriter(store + "_find_keys.txt", true));
                writers.add(findKeyWriter);
            } else {
                findKeyWriter = null;
            }

            this.parseErrorStream = new PrintStream(new FileOutputStream(store + "_parseErrors.txt",
                                                                         true));
        }

        private void flushStreams() throws IOException {
            for(BufferedWriter writer: writers) {
                writer.flush();
            }
            parseErrorStream.flush();
        }

        private void closeStreams() throws IOException {
            for(BufferedWriter writer: writers) {
                writer.close();
            }
            parseErrorStream.close();
            keyFileReader.close();
        }

        private void writeKeyResult(String keyStr, boolean isKeyDeleted) throws IOException {
            successWriter.write(keyStr + "\n");

            if(isKeyDeleted == false) {
                missingKeyWriter.write(keyStr + "\n");
            }
        }

        private static void outputError(BufferedWriter failedKeys,
                                        BufferedWriter errorWriter,
                                        String errorMessage,
                                        Exception e,
                                        String key) throws IOException {

            failedKeys.write(key + "\n");

            errorWriter.write(key + "\n");
            errorWriter.write("Reason for failure " + errorMessage + "\n");
            if(e != null) {
                errorWriter.write("Exception Message " + e.getMessage() + "\n");

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                errorWriter.write("Exception stack " + sw.toString() + "\n");
            }
        }

        public void run() {
            try {
                internalRun();
            } catch(IOException e) {
                System.out.println("Store processing interrupted " + store
                                   + " because of IOException " + e.getMessage());
                e.printStackTrace();
            }
        }

        public void internalRun() throws IOException {
            long currentQps = 0;

            final long BATCH_SIZE = 1000;
            long nextProcessed = BATCH_SIZE;

            String keyStr;
            long totalTimeProcessed = 0;
            long currentTimeMS = System.currentTimeMillis();
            while((keyStr = keyFileReader.readLine()) != null) {
                if(keyStr.length() == 0) {
                    System.out.println("Skipping empty line");
                }

                MutableInt nextParsePos = new MutableInt(0);
                Object key = VoldemortClientShell.parseObject(serializerDef,
                                                              keyStr,
                                                              nextParsePos,
                                                              parseErrorStream);

                if(key == null) {
                    outputError(failedKeyWriter,
                                errorWriter,
                                "Failed to parse Key ####",
                                null,
                                keyStr);
                    continue;
                }

                keysProcessed++;

                if(shouldProcessKey(key) == false) {
                    skipKeyWriter.write(keyStr + "\n");
                    continue;
                }

                if(currentQps >= this.qps) {
                    long elapsedTime = System.currentTimeMillis() - currentTimeMS;
                    currentQps = 0;
                    totalTimeProcessed += elapsedTime;
                    if(elapsedTime <= millisInSeconds) {
                        long remainingMillis = millisInSeconds - elapsedTime;
                        try {
                            Thread.sleep(remainingMillis);
                        } catch(InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    currentTimeMS = System.currentTimeMillis();
                }

                if(totalProcessed >= nextProcessed) {
                    System.out.println("Store " + store + " Processed record count "
                                       + keysProcessed + " elapsed time " + totalTimeProcessed
                                       + " qps " + qps);
                    nextProcessed += BATCH_SIZE;
                    flushStreams();
                }

                currentQps++;
                totalProcessed++;

                try {
                    List<Version> versions = null;
                    boolean hasFetchedVersions = false;
                    if(this.nodeid != -1) {
                        hasFetchedVersions = true;
                        List<Versioned<byte[]>> versionedValues = adminClient.storeOps.getNodeKey(this.store,
                                                                                                  this.nodeid,
                                                                                                  new ByteArray(this.keySerializer.toBytes(key)));
                        versionCallsMade++;
                        if(versionedValues != null && versionedValues.size() > 0) {
                            versions = new ArrayList<Version>();
                            for(Versioned<byte[]> versionedValue: versionedValues) {
                                versions.add(versionedValue.getVersion());
                            }
                        }
                    } else if(this.deleteAllVersions == true || this.checkKeysCount > 0) {
                        hasFetchedVersions = true;
                        versions = getAllVersions(key);
                        versionCallsMade++;
                    }

                    if(hasFetchedVersions) {
                        if(versions != null && versions.size() > 0) {
                            totalVersions += versions.size();
                        } else {
                            // It fetched versions but nothing was returned, so loop around
                            writeKeyResult(keyStr, false);
                            continue;
                        }
                    }

                    boolean isKeyDeleted = true;
                    if(this.checkKeysCount > 0) {
                        for(Version v: versions) {
                            updateKeyCounters(v);
                            String message = "Key " + keyStr + " Version " + v + "\n";
                            findKeyWriter.write(message);
                        }
                        totalKeysFound++;

                        if(totalKeysFound > this.checkKeysCount) {
                            break;
                        }
                    } else if(hasFetchedVersions) {
                        isKeyDeleted = true;
                        for(Version v: versions) {
                            boolean result = client.delete(key, v);
                            isKeyDeleted = isKeyDeleted && result;
                        }
                    } else {
                        isKeyDeleted = client.delete(key);
                    }
                    writeKeyResult(keyStr, isKeyDeleted);

                } catch(Exception e) {
                    outputError(failedKeyWriter,
                                errorWriter,
                                "Error during client delete operation",
                                e,
                                keyStr);
                }
            }

            isComplete = true;
            closeStreams();
        }
    }

    public void deleteKeys() throws Exception {
        Map<String, Thread> workers = new HashMap<String, Thread>();
        Map<String, DeleteKeysTask> tasks = new HashMap<String, DeleteKeysTask>();
        BufferedWriter statusWriter = new BufferedWriter(new FileWriter("status.txt", true));
        for(String store: this.stores) {
            DeleteKeysTask task = new DeleteKeysTask(store,
                                                     storeClients.get(store),
                                                     this.adminClient,
                                                     serializerDefs.get(store),
                                                     this.keyFile,
                                                     this.qps,
                                                     this.nodeid,
                                                     this.deleteAllVersions,
                                                     this.checkKeysCount);
            Thread worker = new Thread(task);
            workers.put(store, worker);
            tasks.put(store, task);
            statusWriter.write("Created thread " + worker.getId() + " for store " + store + "\n");
            System.out.println("Created thread " + worker.getId() + " for store " + store);
            worker.start();
        }
        statusWriter.flush();

        boolean isAllCompleted = false;
        while(isAllCompleted == false) {
            isAllCompleted = true;
            for(String store: stores) {
                Thread worker = workers.get(store);
                DeleteKeysTask task = tasks.get(store);

                System.out.print(task.getStatus());
                statusWriter.write(task.getStatus());
                if(task.isComplete()) {
                    continue;
                }
                if(worker.isAlive() == false) {
                    System.out.println("Thread processing it has died " + store);
                    statusWriter.write("Thread processing it has died " + store + "\n");
                    continue;
                }
                isAllCompleted = false;
            }
            statusWriter.flush();
            if(isAllCompleted == false) {
                Thread.sleep(1000 * 15);
            }
        }
        statusWriter.close();
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = null;
        OptionSet options = null;
        try {
            parser = setupParser();
            options = parser.parse(args);
        } catch(OptionException oe) {
            parser.printHelpOn(System.out);
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
            return;
        }

        /* validate options */
        if(options.has("help")) {
            printUsage();
            return;
        }
        if(!options.hasArgument("url") || !options.hasArgument("stores")
           || !options.hasArgument("keyfile")) {
            printUsageAndDie("Missing a required argument.");
            return;
        }

        boolean deleteAllVersions = options.has("delete-all-versions");
        System.out.println("New Delete All versions value " + deleteAllVersions);

        String url = (String) options.valueOf("url");
        String keyFile = (String) options.valueOf("keyfile");
        keyFile.replace("~", System.getProperty("user.home"));

        int zoneId = Zone.DEFAULT_ZONE_ID;
        if(options.hasArgument("zone")) {
            zoneId = ((Integer) options.valueOf("zone")).intValue();
        }

        int qps = ((Integer) options.valueOf("qps")).intValue();
        int nodeid = ((Integer) options.valueOf("nodeid")).intValue();

        String adminUrl = "";
        if(nodeid != -1) {
            adminUrl = (String) options.valueOf("admin-url");
        }

        List<String> stores = null;
        if(options.hasArgument("stores")) {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) options.valuesOf("stores");
            stores = list;
        }

        int checkKeysCount = 0;
        if(options.hasArgument("check-keys-exist")) {
            checkKeysCount = ((Integer) options.valueOf("check-keys-exist")).intValue();
            if(checkKeysCount <= 0) {
                throw new Exception("Expect a positive value for check-keys-exist");
            }
        }

        DeleteKeysCLI deleteKeysCLI = new DeleteKeysCLI(url,
                                                        adminUrl,
                                                        stores,
                                                        keyFile,
                                                        zoneId,
                                                        qps,
                                                        nodeid,
                                                        deleteAllVersions,
                                                        checkKeysCount);
        deleteKeysCLI.deleteKeys();
    }
}
/*
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ConsistencyCheck {

    private static Logger logger = Logger.getLogger(ConsistencyCheck.class);
    private final List<String> urls;
    private final String storeName;
    private final Integer partitionId;
    private final Reporter reporter;

    private Integer retentionDays = null;
    private Integer replicationFactor = 0;
    private Integer requiredWrites = 0;

    private List<AdminClient> adminClients;
    private List<ClusterNode> clusterNodeList = new ArrayList<ClusterNode>();
    private final Map<ByteArray, Map<Version, Set<ClusterNode>>> keyVersionNodeSetMap = new HashMap<ByteArray, Map<Version, Set<ClusterNode>>>();
    private RetentionChecker retentionChecker;
    private KeyFetchTracker keyFetchTracker;

    public ConsistencyCheck(List<String> urls,
                            String storeName,
                            int partitionId,
                            BufferedWriter badKeyWriter) {
        this.urls = urls;
        this.storeName = storeName;
        this.partitionId = partitionId;
        this.reporter = new Reporter(badKeyWriter);
    }

    /**
     * Connect to the clusters using given urls and start fetching process on
     * correct nodes
     * 
     * @throws Exception When no such store is found
     */

    public void connect() throws Exception {
        adminClients = new ArrayList<AdminClient>(urls.size());
        // bootstrap from two urls
        Map<String, Cluster> clusterMap = new HashMap<String, Cluster>(urls.size());
        Map<String, StoreDefinition> storeDefinitionMap = new HashMap<String, StoreDefinition>(urls.size());

        for(String url: urls) {
            /* connect to cluster through admin port */
            if(logger.isInfoEnabled()) {
                logger.info("Connecting to bootstrap server: " + url);
            }
            AdminClient adminClient = new AdminClient(url,
                                                      new AdminClientConfig(),
                                                      new ClientConfig(),
                                                      0);
            adminClients.add(adminClient);
            /* get Cluster */
            Cluster cluster = adminClient.getAdminClientCluster();
            clusterMap.put(url, cluster);
            /* get StoreDefinition */
            Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
            StoreDefinition storeDefinition = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefinitions.getValue(),
                                                                                              storeName);
            storeDefinitionMap.put(url, storeDefinition);
        }

        /* confirm same number of partitions in all clusters. */
        Integer partitionCount = null;
        for(Entry<String, Cluster> entry: clusterMap.entrySet()) {
            Integer currentPartitionCount = entry.getValue().getNumberOfPartitions();
            if(partitionCount == null) {
                partitionCount = currentPartitionCount;
            }
            if(partitionCount != currentPartitionCount) {
                logger.error("Partition count of different clusters is not the same: "
                             + partitionCount + " vs " + currentPartitionCount);
                throw new VoldemortException("Will not connect because partition counts differ among clusters.");
            }
        }

        /* calculate nodes to scan */
        for(String url: urls) {
            StoreDefinition storeDefinition = storeDefinitionMap.get(url);
            Cluster cluster = clusterMap.get(url);
            Map<Integer, Integer> partitionToNodeMap = ClusterUtils.getCurrentPartitionMapping(cluster);

            /* find list of nodeId hosting partition */
            List<Integer> partitionList = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                             cluster)
                                                                      .getReplicatingPartitionList(partitionId);
            for(int partition: partitionList) {
                Integer nodeId = partitionToNodeMap.get(partition);
                Node node = cluster.getNodeById(nodeId);
                clusterNodeList.add(new ClusterNode(urls.indexOf(url), node));
            }
        }

        /* print config info */
        if(logger.isInfoEnabled()) {
            StringBuilder configInfo = new StringBuilder();
            configInfo.append("TYPE,Store,PartitionId,Node,ZoneId,BootstrapUrl\n");
            for(ClusterNode clusterNode: clusterNodeList) {
                configInfo.append("CONFIG,");
                configInfo.append(storeName + ",");
                configInfo.append(partitionId + ",");
                configInfo.append(clusterNode.getNode().getId() + ",");
                configInfo.append(clusterNode.getNode().getZoneId() + ",");
                configInfo.append(urls.get(clusterNode.getPrefixId()) + "\n");
            }
            for(String line: configInfo.toString().split("\n")) {
                logger.info(line);
            }
        }

        /* calculate retention days and more */
        for(String url: urls) {
            StoreDefinition storeDefinition = storeDefinitionMap.get(url);
            /* retention */
            int storeRetentionDays = 0;
            if(storeDefinition.getRetentionDays() != null) {
                storeRetentionDays = storeDefinition.getRetentionDays().intValue();
            }
            if(retentionDays == null) {
                retentionDays = storeRetentionDays;
            }
            if(retentionDays != storeRetentionDays) {
                if(storeRetentionDays != 0 && (storeRetentionDays < retentionDays)) {
                    retentionDays = storeRetentionDays;
                }
                logger.warn("Retention-days is not consistent between clusters by urls. Will use the shorter.");
            }

            /* replication writes */
            replicationFactor += storeDefinition.getReplicationFactor();

            /* required writes */
            requiredWrites += storeDefinition.getRequiredWrites();
        }
        if(replicationFactor != clusterNodeList.size()) {
            logger.error("Replication factor is not consistent with number of nodes routed to.");
            throw new VoldemortException("Will not connect because replication factor does not accord with number of nodes routed to.");
        }
        retentionChecker = new RetentionChecker(retentionDays);
    }

    /**
     * Run consistency check on connected key-value iterators
     * 
     * @return Results in form of ConsistencyCheckStats
     */
    public Reporter execute() throws IOException {
        Map<ClusterNode, Iterator<Pair<ByteArray, Versioned<byte[]>>>> nodeFetchIteratorMap;
        nodeFetchIteratorMap = new HashMap<ClusterNode, Iterator<Pair<ByteArray, Versioned<byte[]>>>>();
        /* start fetch from each node */
        for(ClusterNode clusterNode: clusterNodeList) {
            AdminClient adminClient = adminClients.get(clusterNode.getPrefixId());
            List<Integer> singlePartition = new ArrayList<Integer>();
            singlePartition.add(partitionId);
            if(logger.isDebugEnabled()) {
                logger.debug("Start fetch request to Node[" + clusterNode.toString()
                             + "] for partition[" + partitionId + "] of store[" + storeName + "]");
            }

            Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchIterator;
            fetchIterator = adminClient.bulkFetchOps.fetchEntries(clusterNode.getNode().getId(),
                                                                  storeName,
                                                                  singlePartition,
                                                                  null,
                                                                  false);
            nodeFetchIteratorMap.put(clusterNode, fetchIterator);
        }
        keyFetchTracker = new KeyFetchTracker(clusterNodeList.size());

        /* start to fetch */
        boolean fetchFinished;
        do {
            fetchFinished = true;
            for(Map.Entry<ClusterNode, Iterator<Pair<ByteArray, Versioned<byte[]>>>> nodeFetchIteratorMapEntry: nodeFetchIteratorMap.entrySet()) {
                ClusterNode clusterNode = nodeFetchIteratorMapEntry.getKey();
                Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchIterator = nodeFetchIteratorMapEntry.getValue();
                if(fetchIterator.hasNext()) {
                    fetchFinished = false;
                    reporter.recordScans(1);

                    Pair<ByteArray, Versioned<byte[]>> fetchedEntry = fetchIterator.next();
                    ByteArray key = fetchedEntry.getFirst();
                    Versioned<byte[]> versioned = fetchedEntry.getSecond();

                    // record fetch
                    recordFetch(clusterNode, key, versioned);

                    // try sweep last key fetched by this iterator
                    keyFetchTracker.recordFetch(clusterNode, key);
                    if(logger.isTraceEnabled()) {
                        logger.trace("fetched " + new String(key.get()));
                        logger.trace("map has keys: " + keyVersionNodeSetMap.size());
                    }
                    trySweepAll();
                    if(logger.isTraceEnabled()) {
                        logger.trace("sweeped; keys left: " + keyVersionNodeSetMap.size());
                    }
                }
            }

            // stats reporting
            if(logger.isInfoEnabled()) {
                String report = reporter.tryProgressReport();
                if(report != null) {
                    for(String line: report.split("\n")) {
                        logger.info(line);
                    }
                }
            }
        } while(!fetchFinished);

        /* adminClient shutdown */
        for(AdminClient adminClient: adminClients) {
            if(adminClient != null) {
                adminClient.close();
            }
        }

        // clean keys not sufficient for write
        cleanIneligibleKeys(keyVersionNodeSetMap, requiredWrites);

        keyFetchTracker.finishAll();
        trySweepAll();

        reporter.processInconsistentKeys(storeName, partitionId, keyVersionNodeSetMap);

        return reporter;
    }

    public void trySweepAll() {
        for(ByteArray finishedKey = keyFetchTracker.nextFinished(); finishedKey != null; finishedKey = keyFetchTracker.nextFinished()) {
            if(keyVersionNodeSetMap.containsKey(finishedKey)) {
                ConsistencyLevel level = determineConsistency(keyVersionNodeSetMap.get(finishedKey),
                                                              replicationFactor);
                if(level == ConsistencyLevel.FULL || level == ConsistencyLevel.LATEST_CONSISTENT) {
                    keyVersionNodeSetMap.remove(finishedKey);
                    reporter.recordGoodKey(1);
                }
            }
        }
    }

    public void recordFetch(ClusterNode clusterNode, ByteArray key, Versioned<byte[]> versioned) {
        Version version;
        if(urls.size() == 1) {
            version = versioned.getVersion();
        } else {
            version = new HashedValue(versioned);
        }

        // skip version if expired
        if(retentionChecker.isExpired(version)) {
            reporter.recordExpired(1);
            return;
        }

        // initialize key -> Map<Version, Set<nodeId>>
        if(!keyVersionNodeSetMap.containsKey(key)) {
            keyVersionNodeSetMap.put(key, new HashMap<Version, Set<ClusterNode>>());
        }
        Map<Version, Set<ClusterNode>> versionNodeSetMap = keyVersionNodeSetMap.get(key);

        // check existing version
        if(!versionNodeSetMap.containsKey(version) && versionNodeSetMap.size() != 0) {
            // if this version is new, sweep old version
            // if this version is old, ignore this version
            Version oneExistingVersion = versionNodeSetMap.keySet().iterator().next();
            if(version.compare(oneExistingVersion) == Occurred.AFTER) {
                versionNodeSetMap.clear();
            } else if(oneExistingVersion.compare(version) == Occurred.AFTER) {
                return;
            }
        }

        if(!versionNodeSetMap.containsKey(version)) {
            // insert nodeSet into the map
            versionNodeSetMap.put(version, new HashSet<ClusterNode>());
        }

        // add node to set
        versionNodeSetMap.get(version).add(clusterNode);
    }

    /**
     * A class to track what keys have been fetched and what keys will not
     * appear any more. It is used to detect keys that will not show up any more
     * so that existing versions can be processed.
     */
    protected static class KeyFetchTracker {

        private final Integer fetcherCount;
        Map<ByteArray, Set<ClusterNode>> fullyFetchedKeyMap = new HashMap<ByteArray, Set<ClusterNode>>();
        Map<ClusterNode, ByteArray> lastFetchedKey = new HashMap<ClusterNode, ByteArray>();
        List<ByteArray> fullyFetchedKeys = new LinkedList<ByteArray>();

        public KeyFetchTracker(Integer fetcherCount) {
            this.fetcherCount = fetcherCount;
        }

        /**
         * Record a fetched result
         * 
         * @param clusterNode The clusterNode from which the key has been
         *        fetched
         * @param key The key itself
         */
        public void recordFetch(ClusterNode clusterNode, ByteArray key) {
            if(lastFetchedKey.containsKey(clusterNode)) {
                ByteArray lastKey = lastFetchedKey.get(clusterNode);
                if(!key.equals(lastKey)) {
                    if(!fullyFetchedKeyMap.containsKey(lastKey)) {
                        fullyFetchedKeyMap.put(lastKey, new HashSet<ClusterNode>());
                    }
                    Set<ClusterNode> lastKeyIterSet = fullyFetchedKeyMap.get(lastKey);
                    lastKeyIterSet.add(clusterNode);

                    // sweep if fully fetched by all iterators
                    if(lastKeyIterSet.size() == fetcherCount) {
                        fullyFetchedKeys.add(lastKey);
                        fullyFetchedKeyMap.remove(lastKey);
                    }
                }
            }
            // remember key fetch states
            lastFetchedKey.put(clusterNode, key);
        }

        /**
         * mark all keys appeared as finished So that they are all in the
         * finished keys queue
         */
        public void finishAll() {
            Set<ByteArray> keySet = new HashSet<ByteArray>();
            keySet.addAll(fullyFetchedKeyMap.keySet());
            keySet.addAll(lastFetchedKey.values());
            fullyFetchedKeys.addAll(keySet);
            fullyFetchedKeyMap.clear();
        }

        /**
         * Get a key that are completed in fetching
         * 
         * @return key considered finished; otherwise null
         */
        public ByteArray nextFinished() {
            if(fullyFetchedKeys.size() > 0) {
                return fullyFetchedKeys.remove(0);
            } else {
                return null;
            }
        }
    }

    protected enum ConsistencyLevel {
        FULL,
        LATEST_CONSISTENT,
        INCONSISTENT,
        EXPIRED,
        INSUFFICIENT_WRITE
    }

    /**
     * Used to track nodes that may share the same nodeId in different clusters
     * 
     */
    protected static class ClusterNode {

        private final Integer clusterId;
        private final Node node;

        /**
         * @param clusterId a prefix to be associated different clusters
         * @param node the real node
         */
        public ClusterNode(Integer clusterId, Node node) {
            this.clusterId = clusterId;
            this.node = node;
        }

        public Integer getPrefixId() {
            return clusterId;
        }

        public Node getNode() {
            return node;
        }

        @Override
        public boolean equals(Object o) {
            if(this == o)
                return true;
            if(!(o instanceof ClusterNode))
                return false;

            ClusterNode n = (ClusterNode) o;
            return clusterId.equals(n.getPrefixId()) && node.equals(n.getNode());
        }

        @Override
        public String toString() {
            return clusterId + "." + node.getId();
        }

    }

    /**
     * A class to save version and value hash It is used to compare versions by
     * the value hash
     * 
     */
    protected static class HashedValue implements Version {

        final private Version innerVersion;
        final private Integer valueHash;

        /**
         * @param versioned Versioned value with version information and value
         *        itself
         */
        public HashedValue(Versioned<byte[]> versioned) {
            innerVersion = versioned.getVersion();
            valueHash = new FnvHashFunction().hash(versioned.getValue());
        }

        public int getValueHash() {
            return valueHash;
        }

        public Version getInner() {
            return innerVersion;
        }

        @Override
        public boolean equals(Object object) {
            if(this == object)
                return true;
            if(object == null)
                return false;
            if(!object.getClass().equals(HashedValue.class))
                return false;
            HashedValue hash = (HashedValue) object;
            boolean result = valueHash.equals(hash.getValueHash());
            return result;
        }

        @Override
        public int hashCode() {
            return valueHash;
        }

        @Override
        public Occurred compare(Version v) {
            return Occurred.CONCURRENTLY; // always regard as conflict
        }
    }

    /**
     * A checker to determine if a key is to be cleaned according to retention
     * policy
     * 
     */
    protected static class RetentionChecker {

        final private long bufferTimeSeconds = 600; // expire N seconds earlier
        final private long expiredTimeMs;

        /**
         * @param days number of days ago from now to retain keys
         */
        public RetentionChecker(int days) {
            if(days <= 0) {
                expiredTimeMs = 0;
            } else {
                long nowMs = System.currentTimeMillis();
                long expirationTimeS = TimeUnit.DAYS.toSeconds(days) - bufferTimeSeconds;
                expiredTimeMs = nowMs - TimeUnit.SECONDS.toMillis(expirationTimeS);
            }
        }

        /**
         * Determine if a version is expired
         * 
         * @param v version to be checked
         * @return if the version is expired according to retention policy
         */
        public boolean isExpired(Version v) {
            if(v instanceof VectorClock) {
                return ((VectorClock) v).getTimestamp() < expiredTimeMs;
            } else if(v instanceof HashedValue) {
                return false;
            } else {
                logger.error("Version type is not supported for checking expiration");
                throw new VoldemortException("Version type is not supported for checking expiration"
                                             + v.getClass().getCanonicalName());
            }
        }
    }

    /**
     * Used to report bad keys, progress, and statistics
     * 
     */
    protected static class Reporter {

        final BufferedWriter badKeyWriter;
        final long reportPeriodMs;

        long lastReportTimeMs = 0;
        long numRecordsScanned = 0;
        long numRecordsScannedLast = 0;
        long numExpiredRecords = 0;
        long numGoodKeys = 0;
        long numTotalKeys = 0;

        /**
         * Will output progress reports every 5 seconds.
         * 
         * @param badKeyWriter Writer to which to output bad keys. Null is OK.
         */
        public Reporter(BufferedWriter badKeyWriter) {
            this(badKeyWriter, 5000);
        }

        /**
         * @param badKeyWriter Writer to which to output bad keys. Null is OK.
         * @param intervalMs Milliseconds between progress reports.
         */
        public Reporter(BufferedWriter badKeyWriter, long intervalMs) {
            this.badKeyWriter = badKeyWriter;
            this.reportPeriodMs = intervalMs;
        }

        public void recordScans(long count) {
            numRecordsScanned += count;
        }

        public void recordExpired(long count) {
            numExpiredRecords += count;
        }

        public String tryProgressReport() {
            if(System.currentTimeMillis() > lastReportTimeMs + reportPeriodMs) {
                long currentTimeMs = System.currentTimeMillis();
                StringBuilder s = new StringBuilder();
                s.append("=====Progress=====\n");
                s.append("Records Scanned: " + numRecordsScanned + "\n");
                s.append("Records Ignored: " + numExpiredRecords + " (Out of Retention)\n");
                s.append("Last Fetch Rate: " + (numRecordsScanned - numRecordsScannedLast)
                         / ((currentTimeMs - lastReportTimeMs) / 1000) + " (records/s)\n");
                lastReportTimeMs = currentTimeMs;
                numRecordsScannedLast = numRecordsScanned;
                return s.toString();
            } else {
                return null;
            }
        }

        public void processInconsistentKeys(String storeName,
                                            Integer partitionId,
                                            Map<ByteArray, Map<Version, Set<ClusterNode>>> keyVersionNodeSetMap)
                throws IOException {
            if(logger.isDebugEnabled()) {
                logger.debug("TYPE,Store,ParId,Key,ServerSet,VersionTS,VectorClock[,ValueHash]");
            }
            for(Map.Entry<ByteArray, Map<Version, Set<ClusterNode>>> entry: keyVersionNodeSetMap.entrySet()) {
                ByteArray key = entry.getKey();
                if(badKeyWriter != null) {
                    badKeyWriter.write(ByteUtils.toHexString(key.get()) + "\n");
                }
                if(logger.isDebugEnabled()) {
                    Map<Version, Set<ClusterNode>> versionMap = entry.getValue();
                    logger.debug(keyVersionToString(key, versionMap, storeName, partitionId));
                }
            }

            recordInconsistentKey(keyVersionNodeSetMap.size());
        }

        public void recordGoodKey(long count) {
            numGoodKeys += count;
            numTotalKeys += count;
        }

        public void recordInconsistentKey(long count) {
            numTotalKeys += count;
        }
    }

    /**
     * Return args parser
     * 
     * @return program parser
     * */
    private static OptionParser getParser() {
        /* parse options */
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("urls", "[REQUIRED] bootstrap URLs")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("partitions", "partition-id")
              .withRequiredArg()
              .describedAs("partition-id")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("store", "store name")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        parser.accepts("bad-key-file", "File name to which inconsistent keys are to be written.")
              .withRequiredArg()
              .describedAs("badKeyFileOut")
              .ofType(String.class);
        return parser;
    }

    /**
     * Print Usage to STDOUT
     */
    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("ConsistencyCheck Tool\n");
        help.append("  Scan partitions of a store by bootstrap url(s) for consistency and\n");
        help.append("  output inconsistent keys to a file.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --partitions <partitionId>[,<partitionId>...]\n");
        help.append("    --urls <url>[,<url>...]\n");
        help.append("    --store <storeName>\n");
        help.append("    --bad-key-file <badKeyFileOut>\n");
        help.append("  Optional:\n");
        help.append("    --help\n");
        help.append("  Note:\n");
        help.append("    If you have two or more clusters to scan for consistency across them,\n");
        help.append("    You will need to supply multiple bootstrap urls, one for each cluster.\n");
        help.append("    When multiple urls are used, all versions are considered as concurrent.\n");
        help.append("    Versioned objects from different nodes are identified by value hashes,\n");
        help.append("    instead of VectorClocks\n");
        help.append("    If specified clusters do not have the same number of partitions, \n");
        help.append("    checking will fail.\n");
        System.out.print(help.toString());
    }

    /**
     * Determine the consistency level of a key
     * 
     * @param versionNodeSetMap A map that maps version to set of PrefixNodes
     * @param replicationFactor Total replication factor for the set of clusters
     * @return ConsistencyLevel Enum
     */
    public static ConsistencyLevel determineConsistency(Map<Version, Set<ClusterNode>> versionNodeSetMap,
                                                        int replicationFactor) {
        boolean fullyConsistent = true;
        Version latestVersion = null;
        for(Map.Entry<Version, Set<ClusterNode>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
            Version version = versionNodeSetEntry.getKey();
            if(version instanceof VectorClock) {
                if(latestVersion == null
                   || ((VectorClock) latestVersion).getTimestamp() < ((VectorClock) version).getTimestamp()) {
                    latestVersion = version;
                }
            }
            Set<ClusterNode> nodeSet = versionNodeSetEntry.getValue();
            fullyConsistent = fullyConsistent && (nodeSet.size() == replicationFactor);
        }
        if(fullyConsistent) {
            return ConsistencyLevel.FULL;
        } else {
            // latest write consistent, effectively consistent
            if(latestVersion != null
               && versionNodeSetMap.get(latestVersion).size() == replicationFactor) {
                return ConsistencyLevel.LATEST_CONSISTENT;
            }
            // all other states inconsistent
            return ConsistencyLevel.INCONSISTENT;
        }
    }

    /**
     * Determine if a key version is invalid by comparing the version's
     * existance and required writes configuration
     * 
     * @param keyVersionNodeSetMap A map that contains keys mapping to a map
     *        that maps versions to set of PrefixNodes
     * @param requiredWrite Required Write configuration
     */
    public static void cleanIneligibleKeys(Map<ByteArray, Map<Version, Set<ClusterNode>>> keyVersionNodeSetMap,
                                           int requiredWrite) {
        Set<ByteArray> keysToDelete = new HashSet<ByteArray>();
        for(Map.Entry<ByteArray, Map<Version, Set<ClusterNode>>> entry: keyVersionNodeSetMap.entrySet()) {
            Set<Version> versionsToDelete = new HashSet<Version>();

            ByteArray key = entry.getKey();
            Map<Version, Set<ClusterNode>> versionNodeSetMap = entry.getValue();
            // mark version for deletion if not enough writes
            for(Map.Entry<Version, Set<ClusterNode>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
                Set<ClusterNode> nodeSet = versionNodeSetEntry.getValue();
                if(nodeSet.size() < requiredWrite) {
                    versionsToDelete.add(versionNodeSetEntry.getKey());
                }
            }
            // delete versions
            for(Version v: versionsToDelete) {
                versionNodeSetMap.remove(v);
            }
            // mark key for deletion if no versions left
            if(versionNodeSetMap.size() == 0) {
                keysToDelete.add(key);
            }
        }
        // delete keys
        for(ByteArray k: keysToDelete) {
            keyVersionNodeSetMap.remove(k);
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        OptionSet options = getParser().parse(args);

        /* validate options */
        if(options.hasArgument("help")) {
            printUsage();
            return;
        }
        if(!options.hasArgument("urls") || !options.hasArgument("partitions")
           || !options.hasArgument("store") || !options.hasArgument("bad-key-file")) {
            printUsage();
            return;
        }

        List<String> urls = (List<String>) options.valuesOf("urls");
        String storeName = (String) options.valueOf("store");
        List<Integer> partitionIds = (List<Integer>) options.valuesOf("partitions");
        String badKeyFile = (String) options.valueOf("bad-key-file");

        BufferedWriter badKeyWriter = null;
        try {
            badKeyWriter = new BufferedWriter(new FileWriter(badKeyFile));
        } catch(IOException e) {
            Utils.croak("Failure to open output file : " + e.getMessage());
        }

        Map<Integer, Reporter> partitionStatsMap = new HashMap<Integer, Reporter>();
        /* scan each partitions */
        try {
            for(Integer partitionId: partitionIds) {
                ConsistencyCheck checker = new ConsistencyCheck(urls,
                                                                storeName,
                                                                partitionId,
                                                                badKeyWriter);
                checker.connect();
                Reporter reporter = checker.execute();
                partitionStatsMap.put(partitionId, reporter);
            }
        } catch(Exception e) {
            Utils.croak("Exception during consistency checking : " + e.getMessage());
        } finally {
            badKeyWriter.close();
        }

        /* print stats */
        StringBuilder statsString = new StringBuilder();
        long totalGoodKeys = 0;
        long totalTotalKeys = 0;
        // each partition
        statsString.append("TYPE,Store,ParitionId,KeysConsistent,KeysTotal,Consistency\n");
        for(Map.Entry<Integer, Reporter> entry: partitionStatsMap.entrySet()) {
            Integer partitionId = entry.getKey();
            Reporter reporter = entry.getValue();
            totalGoodKeys += reporter.numGoodKeys;
            totalTotalKeys += reporter.numTotalKeys;
            statsString.append("STATS,");
            statsString.append(storeName + ",");
            statsString.append(partitionId + ",");
            statsString.append(reporter.numGoodKeys + ",");
            statsString.append(reporter.numTotalKeys + ",");
            statsString.append((double) (reporter.numGoodKeys) / (double) reporter.numTotalKeys);
            statsString.append("\n");
        }
        // all partitions
        statsString.append("STATS,");
        statsString.append(storeName + ",");
        statsString.append("aggregate,");
        statsString.append(totalGoodKeys + ",");
        statsString.append(totalTotalKeys + ",");
        statsString.append((double) (totalGoodKeys) / (double) totalTotalKeys);
        statsString.append("\n");

        for(String line: statsString.toString().split("\n")) {
            logger.info(line);
        }
    }

    /**
     * Convert a key-version-nodeSet information to string
     * 
     * @param key The key
     * @param versionMap mapping versions to set of PrefixNodes
     * @param storeName store's name
     * @param partitionId partition scanned
     * @return a string that describe the information passed in
     */
    public static String keyVersionToString(ByteArray key,
                                            Map<Version, Set<ClusterNode>> versionMap,
                                            String storeName,
                                            Integer partitionId) {
        StringBuilder record = new StringBuilder();
        for(Map.Entry<Version, Set<ClusterNode>> versionSet: versionMap.entrySet()) {
            Version version = versionSet.getKey();
            Set<ClusterNode> nodeSet = versionSet.getValue();

            record.append("BAD_KEY,");
            record.append(storeName + ",");
            record.append(partitionId + ",");
            record.append(ByteUtils.toHexString(key.get()) + ",");
            record.append(nodeSet.toString().replace(", ", ";") + ",");
            if(version instanceof VectorClock) {
                record.append(((VectorClock) version).getTimestamp() + ",");
                record.append(version.toString()
                                     .replaceAll(", ", ";")
                                     .replaceAll(" ts:[0-9]*", "")
                                     .replaceAll("version\\((.*)\\)", "[$1]"));
            }
            if(version instanceof HashedValue) {
                Integer hashValue = ((HashedValue) version).getValueHash();
                Version realVersion = ((HashedValue) version).getInner();
                record.append(((VectorClock) realVersion).getTimestamp() + ",");
                record.append(realVersion.toString()
                                         .replaceAll(", ", ";")
                                         .replaceAll(" ts:[0-9]*", "")
                                         .replaceAll("version\\((.*)\\)", "[$1],"));
                record.append(hashValue);
            }
        }
        return record.toString();
    }

}

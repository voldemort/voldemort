package voldemort.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ConsistencyCheck {

    private enum ConsistencyLevel {
        FULL,
        GREY,
        ORANGE,
        ANTI_DR
    }

    private static class ConsistencyCheckStats {

        private long consistentKeys;
        private long totalKeys;

        public ConsistencyCheckStats() {
            consistentKeys = 0;
            totalKeys = 0;
        }

        public void setConsistentKeys(long count) {
            consistentKeys += count;
        }

        public void setTotalKeys(long count) {
            totalKeys += count;
        }

        public long getConsistentKeys() {
            return consistentKeys;
        }

        public long getTotalKeys() {
            return totalKeys;
        }

        public void append(ConsistencyCheckStats that) {
            consistentKeys += that.getConsistentKeys();
            totalKeys += that.getTotalKeys();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        /* parse options */
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
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
        parser.accepts("primary-zone", "primary zone id").withRequiredArg().ofType(Integer.class);
        parser.accepts("verbose", "verbose");
        OptionSet options = parser.parse(args);

        /* validate options */
        boolean verbose = false;
        if(options.hasArgument("help")) {
            printUsage();
            return;
        }
        if(!options.hasArgument("url") || !options.hasArgument("partitions")
           || !options.hasArgument("store") || !options.hasArgument("primary-zone")) {
            printUsage();
            return;
        }
        if(options.has("verbose")) {
            verbose = true;
        }

        String url = (String) options.valueOf("url");
        String storeName = (String) options.valueOf("store");
        List<Integer> partitionIds = (List<Integer>) options.valuesOf("partitions");
        Integer primaryZoneId = (Integer) options.valueOf("primary-zone");

        ConsistencyCheckStats globalStats = new ConsistencyCheckStats();
        Map<Integer, ConsistencyCheckStats> partitionStatsMap = new HashMap<Integer, ConsistencyCheckStats>();
        for(Integer partitionId: partitionIds) {
            ConsistencyCheckStats partitionStats = doConsistencyCheck(storeName,
                                                                      partitionId,
                                                                      url,
                                                                      primaryZoneId,
                                                                      verbose);
            partitionStatsMap.put(partitionId, partitionStats);
            globalStats.append(partitionStats);
        }

        /* print stats */
        // partition based
        StringBuilder statsString = new StringBuilder();
        // each partition
        statsString.append("TYPE,Store,ParitionId,KeysConsistent,KeysTotal,Consistency\n");
        for(Map.Entry<Integer, ConsistencyCheckStats> entry: partitionStatsMap.entrySet()) {
            Integer partitionId = entry.getKey();
            ConsistencyCheckStats partitionStats = entry.getValue();
            statsString.append("STATS,");
            statsString.append(storeName + ",");
            statsString.append(partitionId + ",");
            statsString.append(partitionStats.getConsistentKeys() + ",");
            statsString.append(partitionStats.getTotalKeys() + ",");
            statsString.append((double) (partitionStats.getConsistentKeys())
                               / (double) partitionStats.getTotalKeys());
            statsString.append("\n");
        }
        // all partitions
        statsString.append("STATS,");
        statsString.append(storeName + ",");
        statsString.append("aggregate,");
        statsString.append(globalStats.getConsistentKeys() + ",");
        statsString.append(globalStats.getTotalKeys() + ",");
        statsString.append((double) (globalStats.getConsistentKeys())
                           / (double) globalStats.getTotalKeys());
        statsString.append("\n");

        System.out.println();
        System.out.println(statsString.toString());
    }

    public static ConsistencyCheckStats doConsistencyCheck(String storeName,
                                                           Integer partitionId,
                                                           String url,
                                                           Integer primaryZoneId,
                                                           boolean verbose) throws Exception {
        List<Integer> singlePartition = new ArrayList<Integer>();
        singlePartition.add(partitionId);

        /* connect to cluster */
        if(verbose) {
            System.out.println("Connecting to bootstrap server: " + url);
        }
        AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), 0);
        Cluster cluster = adminClient.getAdminClientCluster();

        /* find store */
        Versioned<List<StoreDefinition>> storeDefinitions = adminClient.getRemoteStoreDefList(0);
        List<StoreDefinition> StoreDefitions = storeDefinitions.getValue();
        StoreDefinition storeDefinition = null;
        for(StoreDefinition def: StoreDefitions) {
            if(def.getName().equals(storeName)) {
                storeDefinition = def;
                break;
            }
        }
        if(storeDefinition == null) {
            throw new Exception("No such store found: " + storeName);
        }

        /* construct rententionChecker */
        RetentionChecker retentionChecker;
        int retentionDays = 0;
        if(storeDefinition.getRetentionDays() != null) {
            retentionDays = storeDefinition.getRetentionDays().intValue();
        }
        retentionChecker = new RetentionChecker(retentionDays);

        /* make partitionId -> node mapping */
        SortedMap<Integer, Node> partitionToNodeMap = new TreeMap<Integer, Node>();
        Collection<Node> nodes = cluster.getNodes();
        for(Node n: nodes) {
            for(Integer partition: n.getPartitionIds()) {
                if(partitionToNodeMap.containsKey(partition))
                    throw new IllegalArgumentException("Duplicate partition id " + partition
                                                       + " in cluster configuration " + nodes);
                partitionToNodeMap.put(partition, n);
            }
        }

        /* find list of nodeId hosting partition */
        List<Integer> partitionList = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                         cluster)
                                                                  .getReplicatingPartitionList(partitionId);
        List<Integer> nodeIdList = new ArrayList<Integer>(partitionList.size());
        for(int partition: partitionList) {
            Integer nodeId = partitionToNodeMap.get(partition).getId();
            nodeIdList.add(nodeId);
        }

        /* group nodes by zone */
        Map<Integer, Set<Integer>> zoneToNodeIds = new HashMap<Integer, Set<Integer>>();
        for(Integer nodeId: nodeIdList) {
            Integer zoneId = cluster.getNodeById(nodeId).getZoneId();
            if(!zoneToNodeIds.containsKey(zoneId)) {
                zoneToNodeIds.put(zoneId, new HashSet<Integer>());
            }
            zoneToNodeIds.get(zoneId).add(nodeId);
        }

        /* print config info */
        if(verbose) {
            StringBuilder configInfo = new StringBuilder();
            configInfo.append("TYPE,Store,PartitionId,Node,ZoneId\n");
            for(Integer nodeId: nodeIdList) {
                configInfo.append("CONFIG,");
                configInfo.append(storeName + ",");
                configInfo.append(partitionId + ",");
                configInfo.append(nodeId + ",");
                configInfo.append(cluster.getNodeById(nodeId).getZoneId() + "\n");
            }
            System.out.println(configInfo);
        }

        /* get entry Iterator from each node */
        Map<Integer, Iterator<Pair<ByteArray, Versioned<byte[]>>>> nodeEntriesMap;
        nodeEntriesMap = new HashMap<Integer, Iterator<Pair<ByteArray, Versioned<byte[]>>>>();
        for(Integer nodeId: nodeIdList) {
            Iterator<Pair<ByteArray, Versioned<byte[]>>> entries;
            entries = adminClient.fetchEntries(nodeId, storeName, singlePartition, null, false);
            nodeEntriesMap.put(nodeId, entries);
        }

        /* start fetch */
        Map<ByteArray, Map<Version, Set<Integer>>> keyVersionNodeSetMap;
        Map<ByteArray, Set<Iterator<Pair<ByteArray, Versioned<byte[]>>>>> fullyFetchedKeys;
        Map<Iterator<Pair<ByteArray, Versioned<byte[]>>>, ByteArray> lastFetchedKey;
        keyVersionNodeSetMap = new HashMap<ByteArray, Map<Version, Set<Integer>>>();
        fullyFetchedKeys = new HashMap<ByteArray, Set<Iterator<Pair<ByteArray, Versioned<byte[]>>>>>();
        lastFetchedKey = new HashMap<Iterator<Pair<ByteArray, Versioned<byte[]>>>, ByteArray>();

        long numRecordsScanned = 0;
        long numRecordsScannedLast = 0;
        long lastReportTimeMs = 0;
        long reportPeriodMs = 5000;
        long expiredRecords = 0;
        long preQualifiedKeys = 0;
        boolean anyNodeHasNext;
        do {
            anyNodeHasNext = false;
            /* for each iterator */
            for(Map.Entry<Integer, Iterator<Pair<ByteArray, Versioned<byte[]>>>> nodeEntriesMapEntry: nodeEntriesMap.entrySet()) {
                Integer nodeId = nodeEntriesMapEntry.getKey();
                Iterator<Pair<ByteArray, Versioned<byte[]>>> nodeEntries = nodeEntriesMapEntry.getValue();
                if(nodeEntries.hasNext()) {
                    anyNodeHasNext = true;
                    numRecordsScanned++;
                    Pair<ByteArray, Versioned<byte[]>> nodeEntry = nodeEntries.next();
                    ByteArray key = nodeEntry.getFirst();
                    Version version = nodeEntry.getSecond().getVersion();

                    if(retentionChecker.isExpired(version)) {
                        expiredRecords++;
                        continue;
                    } else {
                        // try sweep last key fetched by this iterator
                        if(lastFetchedKey.containsKey(nodeEntries)) {
                            ByteArray lastKey = lastFetchedKey.get(nodeEntries);
                            if(key != lastKey) {
                                if(!fullyFetchedKeys.containsKey(lastKey)) {
                                    fullyFetchedKeys.put(lastKey,
                                                         new HashSet<Iterator<Pair<ByteArray, Versioned<byte[]>>>>());
                                }
                                Set<Iterator<Pair<ByteArray, Versioned<byte[]>>>> lastKeyIterSet = fullyFetchedKeys.get(lastKey);
                                lastKeyIterSet.add(nodeEntries);
                                // sweep if fully fetched by all iterators
                                if(lastKeyIterSet.size() == nodeIdList.size()) {
                                    // keyFetchComplete
                                    ConsistencyLevel level = determineConsistency(keyVersionNodeSetMap.get(lastKey),
                                                                                  storeDefinition,
                                                                                  cluster,
                                                                                  primaryZoneId);
                                    if(level == ConsistencyLevel.FULL) {
                                        keyVersionNodeSetMap.remove(lastKey);
                                        preQualifiedKeys++;
                                    }
                                    fullyFetchedKeys.remove(lastKey);
                                }
                            }
                        }
                        lastFetchedKey.put(nodeEntries, key);
                        // initialize key -> Map<Version, Set<nodeId>>
                        if(!keyVersionNodeSetMap.containsKey(key)) {
                            keyVersionNodeSetMap.put(key, new HashMap<Version, Set<Integer>>());
                        }
                        Map<Version, Set<Integer>> versionNodeSetMap = keyVersionNodeSetMap.get(key);
                        // Initialize Version -> Set<nodeId>
                        if(!versionNodeSetMap.containsKey(version)) {
                            // decide if this is the newest version
                            Iterator<Version> iter = versionNodeSetMap.keySet().iterator();
                            // if after any one in the map, then reset map
                            if(iter.hasNext()) {
                                Version existingVersion = iter.next();
                                // existing version(s) are old
                                if(version.compare(existingVersion) == Occurred.AFTER) {
                                    // swap out the old map and put a new map
                                    versionNodeSetMap = new HashMap<Version, Set<Integer>>();
                                    keyVersionNodeSetMap.put(key, versionNodeSetMap);
                                } else if(existingVersion.compare(version) == Occurred.AFTER) {
                                    // ignore this version
                                    continue;
                                } else if(existingVersion.compare(version) == Occurred.CONCURRENTLY) {

                                } else {
                                    System.err.print("[ERROR]Two versions are not after each other nor currently(key, v1, v2)");
                                    System.err.print(key + ", " + existingVersion + ", " + version);
                                }
                            }
                            // insert nodeIdSet into the map
                            versionNodeSetMap.put(version, new HashSet<Integer>());
                        }
                        // add nodeId to set
                        Set<Integer> nodeSet = versionNodeSetMap.get(version);
                        nodeSet.add(nodeId);
                    }
                }
            }
            // stats reporting
            if(verbose && System.currentTimeMillis() > lastReportTimeMs + reportPeriodMs) {
                long currentTimeMs = System.currentTimeMillis();
                System.out.println("Progress Report");
                System.out.println("===============");
                System.out.println("    Number of records Scanned: " + numRecordsScanned);
                System.out.println("   Records Ignored(Retention): " + expiredRecords);
                System.out.println("Recent fetch speed(records/s): "
                                   + (numRecordsScanned - numRecordsScannedLast)
                                   / ((currentTimeMs - lastReportTimeMs) / 1000));
                System.out.println();
                lastReportTimeMs = currentTimeMs;
                numRecordsScannedLast = numRecordsScanned;
            }
        } while(anyNodeHasNext);

        // analyzing
        if(verbose) {
            System.out.println("Analyzing....");
        }
        cleanIneligibleKeys(keyVersionNodeSetMap, storeDefinition.getRequiredWrites());

        // clean the rest of consistent keys
        Set<ByteArray> keysToDelete = new HashSet<ByteArray>();
        for(ByteArray key: keyVersionNodeSetMap.keySet()) {
            ConsistencyLevel level = determineConsistency(keyVersionNodeSetMap.get(key),
                                                          storeDefinition,
                                                          cluster,
                                                          primaryZoneId);
            if(level == ConsistencyLevel.FULL) {
                keysToDelete.add(key);
            }
        }
        for(ByteArray key: keysToDelete) {
            keyVersionNodeSetMap.remove(key);
            preQualifiedKeys++;
        }

        long totalKeys = keyVersionNodeSetMap.size() + preQualifiedKeys;
        long totalKeysConsistent = preQualifiedKeys;

        // print inconsistent keys
        if(verbose) {
            StringBuilder record = new StringBuilder();
            record.append("TYPE,Store,ParId,Key,ServerSet,VersionTS,VectorClock,ConsistentyLevel\n");
            for(Map.Entry<ByteArray, Map<Version, Set<Integer>>> entry: keyVersionNodeSetMap.entrySet()) {
                ByteArray key = entry.getKey();
                Map<Version, Set<Integer>> versionMap = entry.getValue();
                for(Map.Entry<Version, Set<Integer>> versionSet: versionMap.entrySet()) {
                    Version version = versionSet.getKey();
                    Set<Integer> nodeSet = versionSet.getValue();
                    record.append("BAD_KEY,");
                    record.append(storeName + ",");
                    record.append(partitionId + ",");
                    record.append(ByteUtils.toHexString(key.get()) + ",");
                    record.append(nodeSet.toString().replaceAll(", ", ";") + ",");
                    record.append(((VectorClock) version).getTimestamp() + ",");
                    record.append(version.toString()
                                         .replaceAll(", ", ";")
                                         .replaceAll(" ts:[0-9]*", "")
                                         .replaceAll("version\\((.*)\\)", "[$1]")
                                  + ",");
                    record.append(determineConsistency(versionMap,
                                                       storeDefinition,
                                                       cluster,
                                                       primaryZoneId).toString()
                                  + "\n");
                }
            }
            System.out.println(record.toString());
        }

        ConsistencyCheckStats stats = new ConsistencyCheckStats();
        stats.setConsistentKeys(totalKeysConsistent);
        stats.setTotalKeys(totalKeys);

        return stats;
    }

    public static void printUsage() {
        System.out.println("Usage: \n--partitions <partitionId,partitionId..> --url <url> --store <storeName> --primary-zone <primary-zone-id>");
    }

    public static ConsistencyLevel determineConsistency(Map<Version, Set<Integer>> versionNodeSetMap,
                                                        StoreDefinition storeDef,
                                                        Cluster cluster,
                                                        int primaryZoneId) {
        boolean fullyConsistent = true;
        Version latestVersion = null;
        for(Map.Entry<Version, Set<Integer>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
            Version version = versionNodeSetEntry.getKey();
            if(latestVersion == null
               || ((VectorClock) latestVersion).getTimestamp() < ((VectorClock) version).getTimestamp()) {
                latestVersion = version;
            }
            Set<Integer> nodeSet = versionNodeSetEntry.getValue();
            fullyConsistent = fullyConsistent
                              && (nodeSet.size() == storeDef.getReplicationFactor());
        }
        if(fullyConsistent) {
            return ConsistencyLevel.FULL;
        } else {
            Set<Integer> nodeSet = versionNodeSetMap.get(latestVersion);
            // latest write consistent, effectively consistent
            if(nodeSet.size() == storeDef.getReplicationFactor()) {
                return ConsistencyLevel.GREY;
            }
            // timeout write
            if(nodeSet.size() <= storeDef.getRequiredWrites()) {
                return ConsistencyLevel.GREY;
            }
            // DR-category: if other zone does not have as many available nodes
            // as primary zone
            Map<Integer, Integer> zoneToAvailableNodeCounts = new HashMap<Integer, Integer>();
            for(Integer nodeId: nodeSet) {
                Integer zoneId = cluster.getNodeById(nodeId).getZoneId();
                Integer count = 0;
                if(zoneToAvailableNodeCounts.containsKey(zoneId)) {
                    count = zoneToAvailableNodeCounts.get(zoneId);
                }
                count++;
                zoneToAvailableNodeCounts.put(zoneId, count);
            }
            Integer primaryZoneAvailableNodeCounts = zoneToAvailableNodeCounts.get(primaryZoneId);
            if(primaryZoneAvailableNodeCounts == null) {
                primaryZoneAvailableNodeCounts = 0;
            }
            for(Zone zone: cluster.getZones()) {
                Integer zoneId = zone.getId();
                // if not primary zone and has less nodes available than primary
                // zone
                if(primaryZoneId != zoneId) {
                    if(!zoneToAvailableNodeCounts.containsKey(zoneId)
                       || zoneToAvailableNodeCounts.get(zoneId) == null
                       || zoneToAvailableNodeCounts.get(zoneId) < primaryZoneAvailableNodeCounts) {
                        return ConsistencyLevel.ANTI_DR;
                    }
                }
            }
            // other intermittent inconsistent state
            return ConsistencyLevel.ORANGE;
        }
    }

    public static void cleanIneligibleKeys(Map<ByteArray, Map<Version, Set<Integer>>> keyVersionNodeSetMap,
                                           int requiredWrite) {
        Set<ByteArray> keysToDelete = new HashSet<ByteArray>();
        for(Map.Entry<ByteArray, Map<Version, Set<Integer>>> entry: keyVersionNodeSetMap.entrySet()) {
            Set<Version> versionsToDelete = new HashSet<Version>();

            ByteArray key = entry.getKey();
            Map<Version, Set<Integer>> versionNodeSetMap = entry.getValue();
            // mark version for deletion if not enough writes
            for(Map.Entry<Version, Set<Integer>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
                Set<Integer> nodeSet = versionNodeSetEntry.getValue();
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

    public static class RetentionChecker {

        final long bufferTimeSeconds = 600; // expire N seconds earlier
        final long expiredTimeMs;

        public RetentionChecker(int days) {
            if(days <= 0) {
                expiredTimeMs = 0;
            } else {
                long now = System.currentTimeMillis();
                expiredTimeMs = now - (86400 * days - bufferTimeSeconds) * 1000;
            }
        }

        public boolean isExpired(Version v) {
            if(v instanceof VectorClock) {
                return ((VectorClock) v).getTimestamp() < expiredTimeMs;
            } else {
                System.err.println("[WARNING]Version type is not supported for checking expiration");
                return false;
            }
        }
    }
}

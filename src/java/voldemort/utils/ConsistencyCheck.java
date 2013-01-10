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
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ConsistencyCheck {

    public static void main(String[] args) throws Exception {
        /* parse options */
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("partition", "partition-id")
              .withRequiredArg()
              .describedAs("partition-id")
              .ofType(Integer.class);
        parser.accepts("store", "store name")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
        OptionSet options = parser.parse(args);

        /* validate options */
        if(options.hasArgument("help")) {
            printUsage();
        }
        if(!options.hasArgument("url") || !options.hasArgument("partition")
           || !options.hasArgument("store")) {
            printUsage();
        }

        String url = (String) options.valueOf("url");
        String storeName = (String) options.valueOf("store");
        Integer partitionId = (Integer) options.valueOf("partition");
        List<Integer> singlePartition = new ArrayList<Integer>();
        singlePartition.add(partitionId);

        /* connect to cluster */
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
        StringBuilder configInfo = new StringBuilder();
        configInfo.append("Configuration\n");
        configInfo.append("=============\n");
        configInfo.append("  URL: " + url + "\n");
        configInfo.append("Store: " + storeName + "\n");
        configInfo.append("ParId: " + partitionId + "\n");
        configInfo.append("Nodes: " + nodeIdList.toString() + "\n");
        System.out.println(configInfo);

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
                                if(isConsistent(keyVersionNodeSetMap.get(lastKey),
                                                storeDefinition.getReplicationFactor())) {
                                    keyVersionNodeSetMap.remove(lastKey);
                                    preQualifiedKeys++;
                                }
                                fullyFetchedKeys.remove(lastKey);
                            }
                        }
                    }
                    lastFetchedKey.put(nodeEntries, key);

                    if(retentionChecker.isExpired(version)) {
                        expiredRecords++;
                        continue;
                    } else {
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
            if(System.currentTimeMillis() > lastReportTimeMs + reportPeriodMs) {
                long currentTimeMs = System.currentTimeMillis();
                System.out.println("Progress Report");
                System.out.println("===============");
                System.out.println("    Number of records Scanned: " + numRecordsScanned);
                System.out.println("  Total number of unique keys: " + keyVersionNodeSetMap.size());
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
        System.out.println("Analyzing....");
        cleanIneligibleKeys(keyVersionNodeSetMap, storeDefinition.getRequiredWrites());
        long totalKeys = keyVersionNodeSetMap.size() + preQualifiedKeys;
        long totalKeysConsistent = preQualifiedKeys;

        // print some stats
        StringBuilder totalStats = new StringBuilder();
        totalStats.append("KeysTotal, KeysConsistent, Consistency\n");
        totalStats.append(totalKeys + ", " + totalKeysConsistent + ", ");
        totalStats.append((double) totalKeysConsistent / (double) totalKeys);
        totalStats.append("\n");
        System.out.println(totalStats.toString());

        // zone-wise consistency
        Map<Integer, Integer> zonePartialConsistentKeyCount = new HashMap<Integer, Integer>();
        for(Map.Entry<Integer, Set<Integer>> zoneToNodeSetEntry: zoneToNodeIds.entrySet()) {
            Integer zoneId = zoneToNodeSetEntry.getKey();
            Set<Integer> zoneNodeSet = zoneToNodeSetEntry.getValue();
            int partialConsistentKeyCount = 0;
            for(Map.Entry<ByteArray, Map<Version, Set<Integer>>> entry: keyVersionNodeSetMap.entrySet()) {
                boolean partialConsistent = true;
                for(Set<Integer> keyVersionNodeSet: entry.getValue().values()) {
                    partialConsistent = partialConsistent
                                        && keyVersionNodeSet.containsAll(zoneNodeSet);
                }
                if(partialConsistent) {
                    partialConsistentKeyCount++;
                }
            }
            zonePartialConsistentKeyCount.put(zoneId, partialConsistentKeyCount);
        }

        // print zone-wise consistency result
        System.out.println("storeName,zoneId,consistency");
        for(Integer zoneId: zoneToNodeIds.keySet()) {
            long zoneConsistentKeys = totalKeysConsistent
                                      + zonePartialConsistentKeyCount.get(zoneId);
            System.out.println(storeName + "," + zoneId + "," + (double) (zoneConsistentKeys)
                               / (double) totalKeys);
        }
    }

    public static void printUsage() {
        System.out.println("Usage: \n--partition <partitionId> --url <url> --store <storeName>");
    }

    // public static Map<Integer, Integer> cleanConsistentKeys(Map<ByteArray,
    // Map<Version, Set<Integer>>> keyVersionNodeSetMap,
    // int replicationFactor) {
    // Set<ByteArray> keysToDelete = new HashSet<ByteArray>();
    // Map<Integer, Integer> consistentVersionsCountMap = new HashMap<Integer,
    // Integer>();
    // for(Map.Entry<ByteArray, Map<Version, Set<Integer>>> entry:
    // keyVersionNodeSetMap.entrySet()) {
    // ByteArray key = entry.getKey();
    // Map<Version, Set<Integer>> versionNodeSetMap = entry.getValue();
    // Integer versionCount = versionNodeSetMap.size();
    // boolean consistent = isConsistent(versionNodeSetMap, replicationFactor);
    // if(consistent) {
    // if(consistentVersionsCountMap.containsKey(versionCount)) {
    // Integer count = consistentVersionsCountMap.get(versionCount);
    // consistentVersionsCountMap.put(versionCount, new Integer(count + 1));
    // } else {
    // consistentVersionsCountMap.put(versionCount, new Integer(1));
    // }
    // keyVersionNodeSetMap.put(key, null);
    // keysToDelete.add(key);
    // }
    // }
    // for(ByteArray deleteThisKey: keysToDelete) {
    // keyVersionNodeSetMap.remove(deleteThisKey);
    // }
    // keysToDelete = null;
    // System.gc();
    // return consistentVersionsCountMap;
    // }

    public static boolean isConsistent(Map<Version, Set<Integer>> versionNodeSetMap,
                                       int replicationFactor) {
        boolean consistent = true;
        for(Map.Entry<Version, Set<Integer>> versionNodeSetEntry: versionNodeSetMap.entrySet()) {
            Set<Integer> nodeSet = versionNodeSetEntry.getValue();
            consistent = consistent && (nodeSet.size() == replicationFactor);
        }
        return consistent;
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

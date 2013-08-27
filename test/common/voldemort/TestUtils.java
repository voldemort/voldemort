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

import java.io.File;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.AssertionFailedError;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * Helper utilities for tests
 * 
 * 
 */
public class TestUtils {

    public static final String DIGITS = "0123456789";
    public static final String LETTERS = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
    public static final String CHARACTERS = LETTERS + DIGITS + "~!@#$%^&*()____+-=[];',,,./>?:{}";
    public static final Random SEEDED_RANDOM = new Random(19873482374L);
    public static final Random UNSEEDED_RANDOM = new Random();

    /**
     * Get a vector clock with events on the sequence of nodes given So
     * getClock(1,1,2,2,2) means a clock that has two writes on node 1 and 3
     * writes on node 2.
     * 
     * @param nodes The sequence of nodes
     * @return A VectorClock initialized with the given sequence of events
     */
    public static VectorClock getClock(int... nodes) {
        VectorClock clock = new VectorClock();
        increment(clock, nodes);
        return clock;
    }

    public static VectorClock getClockWithTs(long ts, int... nodes) {
        VectorClock clock = new VectorClock(ts);
        increment(clock, nodes);
        return clock;
    }

    /**
     * Constructs a vector clock as in versioned put based on timestamp
     * 
     * @param timeMs timestamp to use in the clock value
     * @param master node for the put. the master's versions will be timeMs+1,
     *        if master > -1
     * @return
     */
    @SuppressWarnings("deprecation")
    public static VectorClock getVersionedPutClock(long timeMs, int master, int... nodes) {
        List<ClockEntry> clockEntries = Lists.newArrayList();
        for(int node: nodes) {
            if(master >= 0 && node == master) {
                clockEntries.add(new ClockEntry((short) node, timeMs + 1));
            } else {
                clockEntries.add(new ClockEntry((short) node, timeMs));
            }
        }
        return new VectorClock(clockEntries, timeMs);
    }

    /**
     * Helper method to construct Versioned byte value.
     * 
     * @param nodes See getClock method for explanation of this argument
     * @return
     */
    public static Versioned<byte[]> getVersioned(byte[] value, int... nodes) {
        return new Versioned<byte[]>(value, getClock(nodes));
    }

    /**
     * Returns true if both the versioned lists are equal, in terms of values
     * and vector clocks, taking into consideration, they might be in different
     * orders as well. Ignores the timestamps as a part of the vector clock
     * 
     * @param first
     * @param second
     * @return
     */
    public static boolean areVersionedListsEqual(List<Versioned<byte[]>> first,
                                                 List<Versioned<byte[]>> second) {
        if(first.size() != second.size())
            return false;
        // find a match for every first element in second list
        for(Versioned<byte[]> firstElement: first) {
            boolean found = false;
            for(Versioned<byte[]> secondElement: second) {
                if(firstElement.equals(secondElement)) {
                    found = true;
                    break;
                }
            }
            if(!found)
                return false;
        }

        // find a match for every second element in first list
        for(Versioned<byte[]> secondElement: second) {
            boolean found = false;
            for(Versioned<byte[]> firstElement: first) {
                if(firstElement.equals(secondElement)) {
                    found = true;
                    break;
                }
            }
            if(!found)
                return false;
        }
        return true;
    }

    /**
     * Record events for the given sequence of nodes
     * 
     * @param clock The VectorClock to record the events on
     * @param nodes The sequences of node events
     */
    public static void increment(VectorClock clock, int... nodes) {
        for(int n: nodes)
            clock.incrementVersion((short) n, clock.getTimestamp());
    }

    /**
     * Test two byte arrays for (deep) equality. I think this exists in java 6
     * but not java 5
     * 
     * @param a1 Array 1
     * @param a2 Array 2
     * @return True iff a1.length == a2.length and a1[i] == a2[i] for 0 <= i <
     *         a1.length
     */
    public static boolean bytesEqual(byte[] a1, byte[] a2) {
        if(a1 == a2) {
            return true;
        } else if(a1 == null || a2 == null) {
            return false;
        } else if(a1.length != a2.length) {
            return false;
        } else {
            for(int i = 0; i < a1.length; i++)
                if(a1[i] != a2[i])
                    return false;
        }

        return true;
    }

    /**
     * Create a string with some random letters
     * 
     * @param length The length of the string to create
     * @return The string
     */
    public static String randomLetters(int length) {
        return randomString(LETTERS, length);
    }

    /**
     * Create a string that is a random sample (with replacement) from the given
     * string
     * 
     * @param sampler The string to sample from
     * @param length The length of the string to create
     * @return The created string
     */
    public static String randomString(String sampler, int length) {
        StringBuilder builder = new StringBuilder(length);
        for(int i = 0; i < length; i++)
            builder.append(sampler.charAt(SEEDED_RANDOM.nextInt(sampler.length())));
        return builder.toString();
    }

    /**
     * Generate an array of random bytes
     * 
     * @param length
     * @return
     */
    public static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        SEEDED_RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static <K, V, T> void assertContains(Store<K, V, T> store, K key, V... values) {
        List<Versioned<V>> found = store.get(key, null);
        if(found.size() != values.length)
            throw new AssertionFailedError("Expected to find " + values.length
                                           + " values in store, but found only " + found.size()
                                           + ".");
        for(V v: values) {
            boolean isFound = false;
            for(Versioned<V> f: found)
                if(Utils.deepEquals(f.getValue(), v))
                    isFound = true;
            if(!isFound)
                throw new AssertionFailedError("Could not find value " + v + " in results.");
        }
    }

    /**
     * Return an array of length count containing random integers in the range
     * (0, max) generated off the test rng.
     * 
     * @param max The bound on the random number size
     * @param count The number of integers to generate
     * @return The array of integers
     */
    public static int[] randomInts(int max, int count) {
        int[] vals = new int[count];
        for(int i = 0; i < count; i++)
            vals[i] = SEEDED_RANDOM.nextInt(max);
        return vals;
    }

    /**
     * Weirdly java doesn't seem to have Arrays.shuffle(), this terrible hack
     * does that.
     * 
     * @return A shuffled copy of the input
     */
    public static int[] shuffle(int[] input) {
        List<Integer> vals = new ArrayList<Integer>(input.length);
        for(int i = 0; i < input.length; i++)
            vals.add(input[i]);
        Collections.shuffle(vals, SEEDED_RANDOM);
        int[] copy = new int[input.length];
        for(int i = 0; i < input.length; i++)
            copy[i] = vals.get(i);
        return copy;
    }

    /**
     * Compute the requested quantile of the given array
     * 
     * @param values The array of values
     * @param quantile The quantile requested (must be between 0.0 and 1.0
     *        inclusive)
     * @return The quantile
     */
    public static long quantile(long[] values, double quantile) {
        if(values == null)
            throw new IllegalArgumentException("Values cannot be null.");
        if(quantile < 0.0 || quantile > 1.0)
            throw new IllegalArgumentException("Quantile must be between 0.0 and 1.0");

        long[] copy = new long[values.length];
        System.arraycopy(values, 0, copy, 0, copy.length);
        Arrays.sort(copy);
        int index = (int) (copy.length * quantile);
        return copy[index];
    }

    /**
     * Compute the mean of the given values
     * 
     * @param values The values
     * @return The mean
     */
    public static double mean(long[] values) {
        double total = 0.0;
        for(int i = 0; i < values.length; i++)
            total += values[i];
        return total / values.length;
    }

    /**
     * Create a temporary directory in the directory given by java.io.tmpdir
     * 
     * @return The directory created.
     */
    public static File createTempDir() {
        return createTempDir(new File(System.getProperty("java.io.tmpdir")));
    }

    /**
     * Create a temporary directory that is a child of the given directory
     * 
     * @param parent The parent directory
     * @return The temporary directory
     */
    public static File createTempDir(File parent) {
        File temp = new File(parent,
                             Integer.toString(Math.abs(UNSEEDED_RANDOM.nextInt()) % 1000000));
        temp.delete();
        temp.mkdir();
        temp.deleteOnExit();
        return temp;
    }

    /**
     * Wrap the given string in quotation marks. This is slightly more readable
     * then the java inline quotes that require escaping.
     * 
     * @param s The string to wrap in quotes
     * @return The string
     */
    public static String quote(String s) {
        return "\"" + s + "\"";
    }

    public static List<Node> createNodes(int[][] partitionMap) {
        ArrayList<Node> nodes = new ArrayList<Node>(partitionMap.length);
        ArrayList<Integer> partitionList = new ArrayList<Integer>();

        for(int i = 0; i < partitionMap.length; i++) {
            partitionList.clear();
            for(int p = 0; p < partitionMap[i].length; p++) {
                partitionList.add(partitionMap[i][p]);
            }
            nodes.add(new Node(i, "localhost", 8880 + i, 6666 + i, 7000 + i, partitionList));
        }

        return nodes;
    }

    public static int getMissingPartitionsSize(Cluster orig, Cluster updated) {
        int diffPartition = 0;
        ArrayList<Node> nodeAList = new ArrayList<Node>(orig.getNodes());

        for(int i = 0; i < orig.getNodes().size(); i++) {
            Node nodeA = nodeAList.get(i);
            Node nodeB;
            try {
                nodeB = updated.getNodeById(nodeA.getId());
            } catch(VoldemortException e) {
                // add the partition in this node
                diffPartition += nodeA.getNumberOfPartitions();
                continue;
            }

            SortedSet<Integer> bPartitonSet = new TreeSet<Integer>(nodeB.getPartitionIds());
            for(int p: nodeA.getPartitionIds()) {
                if(!bPartitonSet.contains(p)) {
                    diffPartition++;
                }
            }
        }
        return diffPartition;
    }

    /**
     * Given a StoreRoutingPlan generates upto numKeysPerPartition keys per
     * partition
     * 
     * @param numKeysPerPartition
     * @return a hashmap of partition to list of keys generated
     */
    public static HashMap<Integer, List<byte[]>> createPartitionsKeys(StoreRoutingPlan routingPlan,
                                                                      int numKeysPerPartition) {
        HashMap<Integer, List<byte[]>> partitionToKeyList = new HashMap<Integer, List<byte[]>>();
        Set<Integer> partitionsPending = new HashSet<Integer>(routingPlan.getCluster()
                                                                         .getNumberOfPartitions());
        for(int partition = 0; partition < routingPlan.getCluster().getNumberOfPartitions(); partition++) {
            partitionsPending.add(partition);
            partitionToKeyList.put(partition, new ArrayList<byte[]>(numKeysPerPartition));
        }

        for(int key = 0;; key++) {
            byte[] keyBytes = ("key" + key).getBytes();
            int partition = routingPlan.getMasterPartitionId(keyBytes);
            if(partitionToKeyList.get(partition).size() < numKeysPerPartition) {
                partitionToKeyList.get(partition).add(keyBytes);
                if(partitionToKeyList.get(partition).size() == numKeysPerPartition) {
                    partitionsPending.remove(partition);
                }
            }
            if(partitionsPending.size() == 0) {
                break;
            }
        }
        return partitionToKeyList;
    }

    /**
     * Always uses UTF-8.
     */
    public static ByteArray toByteArray(String s) {
        try {
            return new ByteArray(s.getBytes("UTF-8"));
        } catch(UnsupportedEncodingException e) {
            /* Should not happen */
            throw new IllegalStateException(e);
        }
    }

    public static void assertWithBackoff(long timeout, Attempt attempt) throws Exception {
        assertWithBackoff(30, timeout, attempt);
    }

    public static void assertWithBackoff(long initialDelay, long timeout, Attempt attempt)
            throws Exception {
        long delay = initialDelay;
        long finishBy = System.currentTimeMillis() + timeout;

        while(true) {
            try {
                attempt.checkCondition();
                return;
            } catch(AssertionError e) {
                if(System.currentTimeMillis() < finishBy) {
                    Thread.sleep(delay);
                    delay *= 2;
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Because java.beans.ReflectionUtils isn't public...
     */

    @SuppressWarnings("unchecked")
    public static <T> T getPrivateValue(Object instance, String fieldName) throws Exception {
        Field eventDataQueueField = instance.getClass().getDeclaredField(fieldName);
        eventDataQueueField.setAccessible(true);
        return (T) eventDataQueueField.get(instance);
    }

    /**
     * Wrapper to get a StoreDefinition object constructed, given a store name
     */
    public static StoreDefinition makeStoreDefinition(String storeName) {
        return new StoreDefinition(storeName,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   0,
                                   null,
                                   0,
                                   null,
                                   0,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   0);
    }

    /**
     * Wrapper to get a StoreDefinition object constructed, given a store name,
     * memory foot print
     */
    public static StoreDefinition makeStoreDefinition(String storeName, long memFootprintMB) {
        return new StoreDefinition(storeName,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   0,
                                   null,
                                   0,
                                   null,
                                   0,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   memFootprintMB);
    }

    /**
     * Provides a routing strategy for local tests to work with
     * 
     * @return
     */
    public static RoutingStrategy makeSingleNodeRoutingStrategy() {
        Cluster cluster = VoldemortTestConstants.getOneNodeCluster();
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getSingleStoreDefinitionsXml()));
        return new RoutingStrategyFactory().updateRoutingStrategy(storeDefs.get(0), cluster);
    }

    /**
     * Constructs a calendar object representing the given time
     */
    public static GregorianCalendar getCalendar(int year,
                                                int month,
                                                int day,
                                                int hour,
                                                int mins,
                                                int secs) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DATE, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, mins);
        cal.set(Calendar.SECOND, secs);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }
}

/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.routed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.ObsoleteVersionException;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Store which multiplexes requests to different internal StoreClients
 * 
 * The ParallelRoutedStore delegates operations to appropriate sets of
 * sub-clients in parallel.
 * 
 * @author jay
 * 
 */
public class RoutedStore implements Store<byte[], byte[]> {

    private static final long NODE_BANNAGE_MS = 10000L;
    private static final Logger logger = Logger.getLogger(RoutedStore.class.getName());

    private final String name;
    private final Map<Integer, Store<byte[], byte[]>> innerStores;
    private final RoutingStrategy routingStrategy;
    private final int preferredWrites;
    private final int requiredWrites;
    private final int preferredReads;
    private final int requiredReads;
    private final ExecutorService executor;
    private final boolean repairReads;
    private final ReadRepairer<byte[], byte[]> readRepairer;
    private final long timeoutMs;
    private final long nodeBannageMs;
    private final Time time;

    /**
     * Create a RoutedStoreClient
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param routingStrategy The strategy for choosing a node given a key
     * @param requiredReads The minimum number of reads that must complete
     *        before the operation will return
     * @param requiredWrites The minimum number of writes that must complete
     *        before the operation will return
     * @param numberOfThreads The number of threads in the threadpool
     */
    public RoutedStore(String name,
                       Map<Integer, Store<byte[], byte[]>> innerStores,
                       RoutingStrategy routingStrategy,
                       int requiredReads,
                       int requiredWrites,
                       int numberOfThreads,
                       boolean repairReads,
                       long timeoutMs) {
        this(name,
             innerStores,
             routingStrategy,
             requiredReads,
             requiredReads,
             requiredWrites,
             requiredWrites,
             repairReads,
             Executors.newFixedThreadPool(numberOfThreads),
             timeoutMs,
             NODE_BANNAGE_MS,
             SystemTime.INSTANCE);
    }

    /**
     * Create a RoutedStoreClient
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param routingStrategy The strategy for choosing a node given a key
     * @param requiredReads The minimum number of reads that must complete
     *        before the operation will return
     * @param requiredWrites The minimum number of writes that must complete
     *        before the operation will return
     * @param threadPool The threadpool to use
     */
    public RoutedStore(String name,
                       Map<Integer, Store<byte[], byte[]>> innerStores,
                       RoutingStrategy routingStrategy,
                       int preferredReads,
                       int requiredReads,
                       int preferredWrites,
                       int requiredWrites,
                       boolean repairReads,
                       ExecutorService threadPool,
                       long timeoutMs,
                       long nodeBannageMs,
                       Time time) {
        if(requiredReads < 1)
            throw new IllegalArgumentException("Cannot have a requiredReads number less than 1.");
        if(requiredWrites < 1)
            throw new IllegalArgumentException("Cannot have a requiredWrites number less than 1.");
        if(preferredReads < requiredReads)
            throw new IllegalArgumentException("preferredReads must be greater or equal to requiredReads.");
        if(preferredWrites < requiredWrites)
            throw new IllegalArgumentException("preferredWrites must be greater or equal to requiredWrites.");
        if(preferredReads > innerStores.size())
            throw new IllegalArgumentException("preferredReads is larger than the total number of stores!");
        if(preferredWrites > innerStores.size())
            throw new IllegalArgumentException("preferredWrites is larger than the total number of stores!");

        this.name = name;
        this.innerStores = new ConcurrentHashMap<Integer, Store<byte[], byte[]>>(innerStores);
        this.routingStrategy = routingStrategy;
        this.preferredReads = preferredReads;
        this.requiredReads = requiredReads;
        this.preferredWrites = preferredWrites;
        this.requiredWrites = requiredWrites;
        this.repairReads = repairReads;
        this.executor = threadPool;
        this.readRepairer = new ReadRepairer<byte[], byte[]>();
        this.timeoutMs = timeoutMs;
        this.nodeBannageMs = nodeBannageMs;
        this.time = Utils.notNull(time);
    }

    public boolean delete(final byte[] key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = routingStrategy.routeRequest(key);

        // quickly fail if there aren't enough live nodes to meet the
        // requirements
        final int numNodes = nodes.size();
        if(numNodes < this.requiredWrites)
            throw new InsufficientOperationalNodesException("Only " + numNodes
                                                            + " nodes in preference list, but "
                                                            + this.requiredWrites
                                                            + " writes required.");

        // A count of the number of successful operations
        final AtomicInteger successes = new AtomicInteger(0);
        final AtomicBoolean deletedSomething = new AtomicBoolean(false);
        // A list of thrown exceptions, indicating the number of failures
        final List<Exception> failures = Collections.synchronizedList(new LinkedList<Exception>());

        // A semaphore indicating the number of completed operations
        // Once inititialized all permits are acquired, after that
        // permits are released when an operation is completed.
        // semaphore.acquire(n) waits for n operations to complete
        final Semaphore semaphore = new Semaphore(0, false);
        // Add the operations to the pool
        for(final Node node: nodes) {
            this.executor.execute(new Runnable() {

                public void run() {
                    try {
                        boolean deleted = innerStores.get(node.getId()).delete(key, version);
                        successes.incrementAndGet();
                        deletedSomething.compareAndSet(false, deleted);
                        node.getStatus().setAvailable();
                    } catch(UnreachableStoreException e) {
                        failures.add(e);
                        markUnavailable(node, e);
                    } catch(Exception e) {
                        failures.add(e);
                    } finally {
                        // signal that the operation is complete
                        semaphore.release();
                    }
                }
            });
        }

        if(this.preferredWrites <= 0) {
            return true;
        } else {
            for(int i = 0; i < numNodes; i++) {
                try {
                    boolean acquired = semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                    if(!acquired)
                        logger.warn("Delete operation timed out waiting for operation " + i
                                    + "to complete after waiting " + timeoutMs + " ms.");
                    // okay, at least the required number of operations have
                    // completed, were they successful?
                    if(successes.get() >= this.preferredWrites)
                        return deletedSomething.get();
                } catch(InterruptedException e) {
                    throw new InsufficientOperationalNodesException("Delete operation interrupted!",
                                                                    e);
                }
            }
        }

        // If we get to here, that means we couldn't hit the preferred number of
        // writes,
        // throw an exception if you can't even hit the required number
        if(successes.get() < requiredWrites)
            throw new InsufficientOperationalNodesException(this.requiredWrites
                                                                    + " deletes required, but "
                                                                    + successes.get()
                                                                    + " succeeded.",
                                                            failures);
        else
            return deletedSomething.get();
    }

    /*
     * 1. Attempt preferredReads, and then wait for these to complete 2. If we
     * got all the reads we wanted, then we are done. 3. If not then continue
     * serially attempting to read from each node until we get preferredReads or
     * run out of nodes. 4. If we have multiple results do a read repair 5. If
     * we have at least requiredReads return. Otherwise throw an exception.
     */
    public List<Versioned<byte[]>> get(final byte[] key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = routingStrategy.routeRequest(key);

        // quickly fail if there aren't enough nodes to meet the requirement
        if(nodes.size() < this.requiredReads)
            throw new InsufficientOperationalNodesException("Only " + nodes.size()
                                                            + " nodes in preference list, but "
                                                            + this.requiredReads
                                                            + " reads required.");

        final List<Versioned<byte[]>> retrieved = Collections.synchronizedList(new ArrayList<Versioned<byte[]>>());
        final List<NodeValue<byte[], byte[]>> nodeValues = Collections.synchronizedList(new ArrayList<NodeValue<byte[], byte[]>>());

        // A count of the number of successful operations
        final AtomicInteger successes = new AtomicInteger();
        // A list of thrown exceptions, indicating the number of failures
        final List<Exception> failures = Collections.synchronizedList(new LinkedList<Exception>());

        // Do the preferred number of reads in parallel
        final CountDownLatch latch = new CountDownLatch(this.preferredReads);
        int nodeIndex = 0;
        for(; nodeIndex < this.preferredReads; nodeIndex++) {
            final Node node = nodes.get(nodeIndex);
            if(isAvailable(node)) {
                this.executor.execute(new Runnable() {

                    public void run() {
                        try {
                            for(Store store: innerStores.values()) {
                                logger.info("storeID: " + store.getName() + " Class:"
                                            + store.getClass().toString());
                            }
                            List<Versioned<byte[]>> fetched = innerStores.get(node.getId())
                                                                         .get(key);
                            retrieved.addAll(fetched);
                            if(repairReads) {
                                for(Versioned<byte[]> f: fetched)
                                    nodeValues.add(new NodeValue<byte[], byte[]>(node.getId(),
                                                                                 key,
                                                                                 f));
                            }
                            successes.incrementAndGet();
                            node.getStatus().setAvailable();
                        } catch(UnreachableStoreException e) {
                            failures.add(e);
                            markUnavailable(node, e);
                        } catch(Exception e) {
                            logger.debug("Error in get.", e);
                            failures.add(e);
                        } finally {
                            // signal that the operation is complete
                            latch.countDown();
                        }
                    }
                });
            }
        }

        // Wait for those operations to complete or timeout
        try {
            boolean succeeded = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if(!succeeded)
                logger.warn("Get operation timed out after " + timeoutMs + " ms.");
        } catch(InterruptedException e) {
            throw new InsufficientOperationalNodesException("Get operation interrupted!", e);
        }

        // Now if we had any failures we will be short a few reads. Do serial
        // reads to make up for these.
        while(successes.get() < this.preferredReads && nodeIndex < nodes.size()) {
            Node node = nodes.get(nodeIndex);
            try {
                retrieved.addAll(innerStores.get(node.getId()).get(key));
                successes.incrementAndGet();
                node.getStatus().setAvailable();
            } catch(UnreachableStoreException e) {
                failures.add(e);
                markUnavailable(node, e);
            } catch(Exception e) {
                logger.debug("Error in get.", e);
                failures.add(e);
            }
            nodeIndex++;
        }

        // if we have multiple values, do any necessary repairs
        if(repairReads && retrieved.size() > 1) {
            this.executor.execute(new Runnable() {

                public void run() {
                    for(NodeValue<byte[], byte[]> v: readRepairer.getRepairs(nodeValues)) {
                        try {
                            logger.debug("Doing read repair on node " + v.getNodeId()
                                         + " for key '" + v.getKey() + "' with version "
                                         + v.getVersion() + ".");
                            innerStores.get(v.getNodeId()).put(v.getKey(), v.getVersioned());
                        } catch(ObsoleteVersionException e) {
                            logger.debug("Read repair cancelled due to obsolete version on node "
                                         + v.getNodeId() + " for key '" + v.getKey()
                                         + "' with version " + v.getVersion() + ": "
                                         + e.getMessage());
                        } catch(Exception e) {
                            logger.debug("Read repair failed: ", e);
                        }
                    }
                }
            });
        }

        if(successes.get() >= this.requiredReads)
            return retrieved;
        else
            throw new InsufficientOperationalNodesException(this.requiredReads
                                                                    + " reads required, but "
                                                                    + successes.get()
                                                                    + " succeeded.",
                                                            failures);
    }

    public String getName() {
        return this.name;
    }

    public void put(final byte[] key, final Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = routingStrategy.routeRequest(key);

        // quickly fail if there aren't enough nodes to meet the requirement
        final int numNodes = nodes.size();
        if(numNodes < this.requiredWrites)
            throw new InsufficientOperationalNodesException("Only " + numNodes
                                                            + " nodes in preference list, but "
                                                            + this.requiredWrites
                                                            + " writes required.");

        // A count of the number of successful operations
        final AtomicInteger successes = new AtomicInteger(0);

        // A list of thrown exceptions, indicating the number of failures
        final Map<Integer, Exception> failures = Collections.synchronizedMap(new HashMap<Integer, Exception>(1));

        // If requiredWrites > 0 then do a single blocking write to the first
        // live node in the
        // preference list if this node throws an ObsoleteVersionException allow
        // it to propagate
        Node master = null;
        int currentNode = 0;
        Versioned<byte[]> versionedCopy = null;
        for(; currentNode < numNodes; currentNode++) {
            Node current = nodes.get(currentNode);
            if(isAvailable(nodes.get(currentNode))) {
                try {
                    versionedCopy = incremented(versioned, current.getId());
                    innerStores.get(current.getId()).put(key, versionedCopy);
                    successes.getAndIncrement();
                    current.getStatus().setAvailable();
                    master = current;
                    break;
                } catch(UnreachableStoreException e) {
                    markUnavailable(current, e);
                    failures.put(current.getId(), e);
                } catch(ObsoleteVersionException e) {
                    // if this version is obsolete on the master, then bail out
                    // of this operation
                    throw e;
                } catch(Exception e) {
                    failures.put(currentNode, e);
                }
            }
        }

        if(successes.get() < 1)
            throw new InsufficientOperationalNodesException("No master node succeeded!",
                                                            failures.size() > 0 ? failures.get(0)
                                                                               : null);
        else
            currentNode++;

        // A semaphore indicating the number of completed operations
        // Once inititialized all permits are acquired, after that
        // permits are released when an operation is completed.
        // semaphore.acquire(n) waits for n operations to complete
        final Versioned<byte[]> finalVersionedCopy = versionedCopy;
        final Semaphore semaphore = new Semaphore(0, false);
        // Add the operations to the pool
        for(; currentNode < numNodes; currentNode++) {
            final Node node = nodes.get(currentNode);
            if(isAvailable(node)) {
                this.executor.execute(new Runnable() {

                    public void run() {
                        try {
                            innerStores.get(node.getId()).put(key, finalVersionedCopy);
                            successes.incrementAndGet();
                            node.getStatus().setAvailable();
                        } catch(UnreachableStoreException e) {
                            markUnavailable(node, e);
                            failures.put(node.getId(), e);
                        } catch(Exception e) {
                            logger.debug("Error in get.", e);
                            failures.put(node.getId(), e);
                        } finally {
                            // signal that the operation is complete
                            semaphore.release();
                        }
                    }
                });
            }
        }

        // Block until at least requiredwrites have accumulated
        for(int i = 0; i < preferredWrites - 1; i++) {
            try {
                boolean acquired = semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                if(!acquired)
                    logger.warn("Timed out waiting for put to succeed.");
                // okay, at least the required number of operations have
                // completed, where they successful?
                if(successes.get() >= this.preferredWrites)
                    break;
            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException("Put operation interrupted");
            }
        }

        // if we don't have enough writes, then do some slop writes
        // these are seperate because we only want to do them if we have to
        if(successes.get() < requiredWrites) {
            throw new InsufficientOperationalNodesException(successes.get()
                                                            + " writes succeeded, but "
                                                            + this.requiredWrites
                                                            + " are required.", failures.values());
        }

        // Okay looks like it worked, increment the version for the caller
        VectorClock versionedClock = (VectorClock) versioned.getVersion();
        versionedClock.incrementVersion(master.getId(), time.getMilliseconds());
    }

    private boolean isAvailable(Node node) {
        return !node.getStatus().isUnavailable(this.nodeBannageMs);
    }

    private void markUnavailable(Node node, Exception e) {
        logger.warn("Could not connect to node " + node.getId() + " at " + node.getHost()
                    + " marking as unavailable for " + this.nodeBannageMs + " ms.");
        logger.debug(e);
        node.getStatus().setUnavailable();
    }

    private Versioned<byte[]> incremented(Versioned<byte[]> versioned, int nodeId) {
        return new Versioned<byte[]>(versioned.getValue(),
                                     ((VectorClock) versioned.getVersion()).incremented(nodeId,
                                                                                        time.getMilliseconds()));
    }

    public void close() {
        this.executor.shutdown();
        try {
            this.executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            // okay, fine, playing nice didn't work
            this.executor.shutdownNow();
        }
        VoldemortException exception = null;
        for(Store<byte[], byte[]> client: innerStores.values()) {
            try {
                client.close();
            } catch(VoldemortException v) {
                exception = v;
            }
        }
        if(exception != null)
            throw exception;
    }

    Map<Integer, Store<byte[], byte[]>> getInnerStores() {
        return this.innerStores;
    }

}

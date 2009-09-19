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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * @author jay
 * 
 */
public class RoutedStore implements Store<ByteArray, byte[]> {

    private static final long NODE_BANNAGE_MS = 10000L;
    private static final Logger logger = Logger.getLogger(RoutedStore.class.getName());

    private final static StoreOp<Versioned<byte[]>> VERSIONED_OP = new StoreOp<Versioned<byte[]>>() {

        public List<Versioned<byte[]>> execute(Store<ByteArray, byte[]> store, ByteArray key) {
            return store.get(key);
        }
    };

    private final static StoreOp<Version> VERSION_OP = new StoreOp<Version>() {

        public List<Version> execute(Store<ByteArray, byte[]> store, ByteArray key) {
            return store.getVersions(key);
        }
    };

    private final String name;
    private final Map<Integer, Store<ByteArray, byte[]>> innerStores;
    private final ExecutorService executor;
    private final boolean repairReads;
    private final ReadRepairer<ByteArray, byte[]> readRepairer;
    private final long timeoutMs;
    private final long nodeBannageMs;
    private final Time time;
    private final Cluster cluster;
    private final StoreDefinition storeDef;

    private final RoutingStrategy routingStrategy;

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
                       Map<Integer, Store<ByteArray, byte[]>> innerStores,
                       Cluster cluster,
                       StoreDefinition storeDef,
                       int numberOfThreads,
                       boolean repairReads,
                       long timeoutMs) {
        this(name,
             innerStores,
             cluster,
             storeDef,
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
                       Map<Integer, Store<ByteArray, byte[]>> innerStores,
                       Cluster cluster,
                       StoreDefinition storeDef,
                       boolean repairReads,
                       ExecutorService threadPool,
                       long timeoutMs,
                       long nodeBannageMs,
                       Time time) {
        if(storeDef.getRequiredReads() < 1)
            throw new IllegalArgumentException("Cannot have a storeDef.getRequiredReads() number less than 1.");
        if(storeDef.getRequiredWrites() < 1)
            throw new IllegalArgumentException("Cannot have a storeDef.getRequiredWrites() number less than 1.");
        if(storeDef.getPreferredReads() < storeDef.getRequiredReads())
            throw new IllegalArgumentException("storeDef.getPreferredReads() must be greater or equal to storeDef.getRequiredReads().");
        if(storeDef.getPreferredWrites() < storeDef.getRequiredWrites())
            throw new IllegalArgumentException("storeDef.getPreferredWrites() must be greater or equal to storeDef.getRequiredWrites().");
        if(storeDef.getPreferredReads() > innerStores.size())
            throw new IllegalArgumentException("storeDef.getPreferredReads() is larger than the total number of nodes!");
        if(storeDef.getPreferredWrites() > innerStores.size())
            throw new IllegalArgumentException("storeDef.getPreferredWrites() is larger than the total number of nodes!");

        this.name = name;
        this.innerStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[]>>(innerStores);
        this.repairReads = repairReads;
        this.executor = threadPool;
        this.readRepairer = new ReadRepairer<ByteArray, byte[]>();
        this.timeoutMs = timeoutMs;
        this.nodeBannageMs = nodeBannageMs;
        this.time = Utils.notNull(time);
        this.cluster = cluster;
        this.storeDef = storeDef;

        this.routingStrategy = new RoutingStrategyFactory(this.cluster).getRoutingStrategy(storeDef);
    }

    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));

        // quickly fail if there aren't enough live nodes to meet the
        // requirements
        final int numNodes = nodes.size();
        if(numNodes < this.storeDef.getRequiredWrites())
            throw new InsufficientOperationalNodesException("Only " + numNodes
                                                            + " nodes in preference list, but "
                                                            + this.storeDef.getRequiredWrites()
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
                        logger.warn("Error in DELETE on node " + node.getId() + "("
                                    + node.getHost() + ")", e);
                    } finally {
                        // signal that the operation is complete
                        semaphore.release();
                    }
                }
            });
        }

        int attempts = Math.min(storeDef.getPreferredWrites(), numNodes);
        if(this.storeDef.getPreferredWrites() <= 0) {
            return true;
        } else {
            for(int i = 0; i < numNodes; i++) {
                try {
                    boolean acquired = semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
                    if(!acquired)
                        logger.warn("Delete operation timed out waiting for operation " + i
                                    + " to complete after waiting " + timeoutMs + " ms.");
                    // okay, at least the required number of operations have
                    // completed, were they successful?
                    if(successes.get() >= attempts)
                        return deletedSomething.get();
                } catch(InterruptedException e) {
                    throw new InsufficientOperationalNodesException("Delete operation interrupted!",
                                                                    e);
                }
            }
        }

        // If we get to here, that means we couldn't hit the preferred number
        // of writes, throw an exception if you can't even hit the required
        // number
        if(successes.get() < storeDef.getRequiredWrites())
            throw new InsufficientOperationalNodesException(this.storeDef.getRequiredWrites()
                                                                    + " deletes required, but "
                                                                    + successes.get()
                                                                    + " succeeded.",
                                                            failures);
        else
            return deletedSomething.get();
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);

        // Keys for each node needed to satisfy storeDef.getPreferredReads() if
        // no failures.
        Map<Node, List<ByteArray>> nodeToKeysMap = Maps.newHashMap();

        // Keep track of nodes per key that might be needed if there are
        // failures during getAll
        Map<ByteArray, List<Node>> keyToExtraNodesMap = Maps.newHashMap();

        for(ByteArray key: keys) {
            List<Node> availableNodes = availableNodes(routingStrategy.routeRequest(key.get()));

            // quickly fail if there aren't enough nodes to meet the requirement
            checkRequiredReads(availableNodes);
            int preferredReads = storeDef.getPreferredReads();
            List<Node> preferredNodes = Lists.newArrayListWithCapacity(preferredReads);
            List<Node> extraNodes = Lists.newArrayListWithCapacity(3);

            for(Node node: availableNodes) {
                if(preferredNodes.size() < preferredReads)
                    preferredNodes.add(node);
                else
                    extraNodes.add(node);
            }

            for(Node node: preferredNodes) {
                List<ByteArray> nodeKeys = nodeToKeysMap.get(node);
                if(nodeKeys == null) {
                    nodeKeys = Lists.newArrayList();
                    nodeToKeysMap.put(node, nodeKeys);
                }
                nodeKeys.add(key);
            }
            if(!extraNodes.isEmpty()) {
                List<Node> nodes = keyToExtraNodesMap.get(key);
                if(nodes == null)
                    keyToExtraNodesMap.put(key, extraNodes);
                else
                    nodes.addAll(extraNodes);
            }
        }

        List<Callable<GetAllResult>> callables = Lists.newArrayList();
        for(Map.Entry<Node, List<ByteArray>> entry: nodeToKeysMap.entrySet()) {
            final Node node = entry.getKey();
            final Collection<ByteArray> nodeKeys = entry.getValue();
            if(isAvailable(node))
                callables.add(new GetAllCallable(node, nodeKeys));
        }

        // A list of thrown exceptions, indicating the number of failures
        List<Throwable> failures = Lists.newArrayList();
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();

        Map<ByteArray, MutableInt> keyToSuccessCount = Maps.newHashMap();
        for(ByteArray key: keys)
            keyToSuccessCount.put(key, new MutableInt(0));

        List<Future<GetAllResult>> futures;
        try {
            // TODO What to do about timeouts? They should be longer as getAll
            // is likely to
            // take longer. At the moment, it's just timeoutMs * 3, but should
            // this be based on the number of the keys?
            futures = executor.invokeAll(callables, timeoutMs * 3, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            throw new InsufficientOperationalNodesException("getAll operation interrupted.", e);
        }
        for(Future<GetAllResult> f: futures) {
            if(f.isCancelled()) {
                logger.warn("Get operation timed out after " + timeoutMs + " ms.");
                continue;
            }
            try {
                GetAllResult getResult = f.get();
                if(getResult.exception != null) {
                    failures.add(getResult.exception);
                    continue;
                }
                for(ByteArray key: getResult.callable.nodeKeys) {
                    List<Versioned<byte[]>> retrieved = getResult.retrieved.get(key);
                    MutableInt successCount = keyToSuccessCount.get(key);
                    successCount.increment();

                    /*
                     * retrieved can be null if there are no values for the key
                     * provided
                     */
                    if(retrieved != null) {
                        List<Versioned<byte[]>> existing = result.get(key);
                        if(existing == null)
                            result.put(key, Lists.newArrayList(retrieved));
                        else
                            existing.addAll(retrieved);
                    }
                }
                nodeValues.addAll(getResult.nodeValues);

            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException("getAll operation interrupted.", e);
            } catch(ExecutionException e) {
                // We catch all Throwables apart from Error in the callable, so
                // the else part
                // should never happen
                if(e.getCause() instanceof Error)
                    throw (Error) e.getCause();
                else
                    logger.error(e.getMessage(), e);
            }
        }

        for(ByteArray key: keys) {
            MutableInt successCountWrapper = keyToSuccessCount.get(key);
            int successCount = successCountWrapper.intValue();
            if(successCount < storeDef.getPreferredReads()) {
                List<Node> extraNodes = keyToExtraNodesMap.get(key);
                if(extraNodes != null) {
                    for(Node node: extraNodes) {
                        try {
                            List<Versioned<byte[]>> values = innerStores.get(node.getId()).get(key);
                            fillRepairReadsValues(nodeValues, key, node, values);
                            List<Versioned<byte[]>> versioneds = result.get(key);
                            if(versioneds == null)
                                result.put(key, Lists.newArrayList(values));
                            else
                                versioneds.addAll(values);
                            node.getStatus().setAvailable();
                            if(++successCount >= storeDef.getPreferredReads())
                                break;

                        } catch(UnreachableStoreException e) {
                            failures.add(e);
                            markUnavailable(node, e);
                        } catch(Exception e) {
                            logger.warn("Error in GET_ALL on node " + node.getId() + "("
                                        + node.getHost() + ")", e);
                            failures.add(e);
                        }
                    }
                }
            }
            successCountWrapper.setValue(successCount);
        }

        repairReads(nodeValues);

        for(Map.Entry<ByteArray, MutableInt> mapEntry: keyToSuccessCount.entrySet()) {
            int successCount = mapEntry.getValue().intValue();
            if(successCount < storeDef.getRequiredReads())
                throw new InsufficientOperationalNodesException(this.storeDef.getRequiredReads()
                                                                        + " reads required, but "
                                                                        + successCount
                                                                        + " succeeded.",
                                                                failures);
        }

        return result;
    }

    public List<Versioned<byte[]>> get(ByteArray key) {
        Function<List<GetResult<Versioned<byte[]>>>, Void> readRepairFunction = new Function<List<GetResult<Versioned<byte[]>>>, Void>() {

            public Void apply(List<GetResult<Versioned<byte[]>>> nodeResults) {
                List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayListWithExpectedSize(nodeResults.size());
                for(GetResult<Versioned<byte[]>> getResult: nodeResults)
                    fillRepairReadsValues(nodeValues,
                                          getResult.key,
                                          getResult.node,
                                          getResult.retrieved);
                repairReads(nodeValues);
                return null;
            }
        };
        return get(key, VERSIONED_OP, readRepairFunction);
    }

    /*
     * 1. Attempt preferredReads, and then wait for these to complete 2. If we
     * got all the reads we wanted, then we are done. 3. If not then continue
     * serially attempting to read from each node until we get preferredReads or
     * run out of nodes. 4. If we have multiple results do a read repair 5. If
     * we have at least requiredReads return. Otherwise throw an exception.
     */
    private <R> List<R> get(final ByteArray key,
                            StoreOp<R> fetcher,
                            Function<List<GetResult<R>>, Void> preReturnProcedure)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));

        // quickly fail if there aren't enough nodes to meet the requirement
        checkRequiredReads(nodes);

        final List<GetResult<R>> retrieved = Lists.newArrayList();

        // A count of the number of successful operations
        int successes = 0;
        // A list of thrown exceptions, indicating the number of failures
        final List<Throwable> failures = Lists.newArrayListWithCapacity(3);

        // Do the preferred number of reads in parallel
        int attempts = Math.min(this.storeDef.getPreferredReads(), nodes.size());
        int nodeIndex = 0;
        List<Callable<GetResult<R>>> callables = Lists.newArrayListWithCapacity(attempts);
        for(; nodeIndex < attempts; nodeIndex++) {
            final Node node = nodes.get(nodeIndex);
            callables.add(new GetCallable<R>(node, key, fetcher));
        }

        List<Future<GetResult<R>>> futures;
        try {
            futures = executor.invokeAll(callables, timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            throw new InsufficientOperationalNodesException("Get operation interrupted!", e);
        }

        for(Future<GetResult<R>> f: futures) {
            if(f.isCancelled()) {
                logger.warn("Get operation timed out after " + timeoutMs + " ms.");
                continue;
            }
            try {
                GetResult<R> getResult = f.get();
                if(getResult.exception != null) {
                    failures.add(getResult.exception);
                    continue;
                }
                ++successes;
                retrieved.add(getResult);
            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException("Get operation interrupted!", e);
            } catch(ExecutionException e) {
                // We catch all Throwable subclasses apart from Error in the
                // callable, so the else
                // part should never happen.
                if(e.getCause() instanceof Error)
                    throw (Error) e.getCause();
                else
                    logger.error(e.getMessage(), e);
            }
        }

        // Now if we had any failures we will be short a few reads. Do serial
        // reads to make up for these.
        while(successes < this.storeDef.getPreferredReads() && nodeIndex < nodes.size()) {
            Node node = nodes.get(nodeIndex);
            try {
                retrieved.add(new GetResult<R>(node,
                                               key,
                                               fetcher.execute(innerStores.get(node.getId()), key),
                                               null));
                ++successes;
                node.getStatus().setAvailable();
            } catch(UnreachableStoreException e) {
                failures.add(e);
                markUnavailable(node, e);
            } catch(Exception e) {
                logger.warn("Error in GET on node " + node.getId() + "(" + node.getHost() + ")", e);
                failures.add(e);
            }
            nodeIndex++;
        }

        if(logger.isTraceEnabled())
            logger.trace("GET retrieved the following node values: " + formatNodeValues(retrieved));

        if(preReturnProcedure != null)
            preReturnProcedure.apply(retrieved);

        if(successes >= this.storeDef.getRequiredReads()) {
            List<R> result = Lists.newArrayListWithExpectedSize(retrieved.size());
            for(GetResult<R> getResult: retrieved)
                result.addAll(getResult.retrieved);
            return result;
        } else
            throw new InsufficientOperationalNodesException(this.storeDef.getRequiredReads()
                                                            + " reads required, but " + successes
                                                            + " succeeded.", failures);
    }

    private void fillRepairReadsValues(final List<NodeValue<ByteArray, byte[]>> nodeValues,
                                       final ByteArray key,
                                       Node node,
                                       List<Versioned<byte[]>> fetched) {
        if(repairReads) {
            if(fetched.size() == 0)
                nodeValues.add(nullValue(node, key));
            else {
                for(Versioned<byte[]> f: fetched)
                    nodeValues.add(new NodeValue<ByteArray, byte[]>(node.getId(), key, f));
            }
        }
    }

    private NodeValue<ByteArray, byte[]> nullValue(Node node, ByteArray key) {
        return new NodeValue<ByteArray, byte[]>(node.getId(), key, new Versioned<byte[]>(null));
    }

    private void repairReads(final List<NodeValue<ByteArray, byte[]>> nodeValues) {
        if(!repairReads || nodeValues.size() <= 1 || storeDef.getPreferredReads() <= 1)
            return;

        this.executor.execute(new Runnable() {

            public void run() {
                for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
                    try {
                        if(logger.isDebugEnabled())
                            logger.debug("Doing read repair on node " + v.getNodeId()
                                         + " for key '" + v.getKey() + "' with version "
                                         + v.getVersion() + ".");
                        innerStores.get(v.getNodeId()).put(v.getKey(), v.getVersioned());
                    } catch(ObsoleteVersionException e) {
                        if(logger.isDebugEnabled())
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

    private void checkRequiredReads(final List<Node> nodes)
            throws InsufficientOperationalNodesException {
        if(nodes.size() < this.storeDef.getRequiredReads())
            throw new InsufficientOperationalNodesException("Only " + nodes.size()
                                                            + " nodes in preference list, but "
                                                            + this.storeDef.getRequiredReads()
                                                            + " reads required.");
    }

    private <R> String formatNodeValues(List<GetResult<R>> results) {
        // log all retrieved values
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(GetResult<?> r: results) {
            builder.append("GetResult(nodeId=" + r.node.getId() + ", key=" + r.key
                           + ", retrieved= " + r.retrieved + ")");
            builder.append(", ");
        }
        builder.append("}");

        return builder.toString();
    }

    public String getName() {
        return this.name;
    }

    public void put(final ByteArray key, final Versioned<byte[]> versioned)
            throws VoldemortException {
        long startNs = System.nanoTime();
        StoreUtils.assertValidKey(key);
        final List<Node> nodes = availableNodes(routingStrategy.routeRequest(key.get()));

        // quickly fail if there aren't enough nodes to meet the requirement
        final int numNodes = nodes.size();
        if(numNodes < this.storeDef.getRequiredWrites())
            throw new InsufficientOperationalNodesException("Only " + numNodes
                                                            + " nodes in preference list, but "
                                                            + this.storeDef.getRequiredWrites()
                                                            + " writes required.");

        // A count of the number of successful operations
        final AtomicInteger successes = new AtomicInteger(0);

        // A list of thrown exceptions, indicating the number of failures
        final List<Exception> failures = Collections.synchronizedList(new ArrayList<Exception>(1));

        // If requiredWrites > 0 then do a single blocking write to the first
        // live node in the preference list if this node throws an
        // ObsoleteVersionException allow it to propagate
        Node master = null;
        int currentNode = 0;
        Versioned<byte[]> versionedCopy = null;
        for(; currentNode < numNodes; currentNode++) {
            Node current = nodes.get(currentNode);
            try {
                versionedCopy = incremented(versioned, current.getId());
                innerStores.get(current.getId()).put(key, versionedCopy);
                successes.getAndIncrement();
                current.getStatus().setAvailable();
                master = current;
                break;
            } catch(UnreachableStoreException e) {
                markUnavailable(current, e);
                failures.add(e);
            } catch(ObsoleteVersionException e) {
                // if this version is obsolete on the master, then bail out
                // of this operation
                throw e;
            } catch(Exception e) {
                failures.add(e);
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
        int attempts = 0;
        for(; currentNode < numNodes; currentNode++) {
            attempts++;
            final Node node = nodes.get(currentNode);
            this.executor.execute(new Runnable() {

                public void run() {
                    try {
                        innerStores.get(node.getId()).put(key, finalVersionedCopy);
                        successes.incrementAndGet();
                        node.getStatus().setAvailable();
                    } catch(UnreachableStoreException e) {
                        markUnavailable(node, e);
                        failures.add(e);
                    } catch(Exception e) {
                        logger.warn("Error in PUT on node " + node.getId() + "(" + node.getHost()
                                    + ")", e);
                        failures.add(e);
                    } finally {
                        // signal that the operation is complete
                        semaphore.release();
                    }
                }
            });
        }

        // Block until we get enough completions
        int blockCount = Math.min(storeDef.getPreferredWrites() - 1, attempts);
        boolean noTimeout = blockOnPut(startNs,
                                       semaphore,
                                       0,
                                       blockCount,
                                       successes,
                                       storeDef.getPreferredWrites());

        if(successes.get() < storeDef.getRequiredWrites()) {
            /*
             * We don't have enough required writes, but we haven't timed out
             * yet, so block a little more if there are healthy nodes that can
             * help us achieve our target.
             */
            if(noTimeout) {
                int startingIndex = blockCount - 1;
                blockCount = Math.max(storeDef.getPreferredWrites() - 1, attempts);
                blockOnPut(startNs,
                           semaphore,
                           startingIndex,
                           blockCount,
                           successes,
                           storeDef.getRequiredWrites());
            }
            if(successes.get() < storeDef.getRequiredWrites())
                throw new InsufficientOperationalNodesException(successes.get()
                                                                + " writes succeeded, but "
                                                                + this.storeDef.getRequiredWrites()
                                                                + " are required.", failures);
        }

        // Okay looks like it worked, increment the version for the caller
        VectorClock versionedClock = (VectorClock) versioned.getVersion();
        versionedClock.incrementVersion(master.getId(), time.getMilliseconds());
    }

    /**
     * @return false if the operation timed out, true otherwise.
     */
    private boolean blockOnPut(long startNs,
                               Semaphore semaphore,
                               int startingIndex,
                               int blockCount,
                               AtomicInteger successes,
                               int successesRequired) {
        for(int i = startingIndex; i < blockCount; i++) {
            try {
                long ellapsedNs = System.nanoTime() - startNs;
                long remainingNs = (timeoutMs * Time.NS_PER_MS) - ellapsedNs;
                boolean acquiredPermit = semaphore.tryAcquire(Math.max(remainingNs, 0),
                                                              TimeUnit.NANOSECONDS);
                if(!acquiredPermit) {
                    logger.warn("Timed out waiting for put # " + (i + 1) + " of " + blockCount
                                + " to succeed.");
                    return false;
                }
                if(successes.get() >= successesRequired)
                    break;
            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException("Put operation interrupted", e);
            }
        }
        return true;
    }

    private boolean isAvailable(Node node) {
        return !node.getStatus().isUnavailable(this.nodeBannageMs);
    }

    private void markUnavailable(Node node, UnreachableStoreException e) {
        logger.warn("Could not connect to node " + node.getId() + " at " + node.getHost()
                    + " marking as unavailable for " + this.nodeBannageMs + " ms.", e);
        logger.debug(e);
        node.getStatus().setUnavailable();
    }

    private Versioned<byte[]> incremented(Versioned<byte[]> versioned, int nodeId) {
        return new Versioned<byte[]>(versioned.getValue(),
                                     ((VectorClock) versioned.getVersion()).incremented(nodeId,
                                                                                        time.getMilliseconds()));
    }

    private List<Node> availableNodes(List<Node> list) {
        List<Node> available = new ArrayList<Node>(list.size());
        for(Node node: list)
            if(isAvailable(node))
                available.add(node);
        return available;
    }

    public void close() {
        this.executor.shutdown();
        try {
            if(!this.executor.awaitTermination(10, TimeUnit.SECONDS))
                this.executor.shutdownNow();
        } catch(InterruptedException e) {
            // okay, fine, playing nice didn't work
            this.executor.shutdownNow();
        }
        VoldemortException exception = null;
        for(Store<?, ?> client: innerStores.values()) {
            try {
                client.close();
            } catch(VoldemortException v) {
                exception = v;
            }
        }
        if(exception != null)
            throw exception;
    }

    Map<Integer, Store<ByteArray, byte[]>> getInnerStores() {
        return this.innerStores;
    }

    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROUTING_STRATEGY:
                return this.routingStrategy;
            case READ_REPAIRER:
                return this.readRepairer;
            case VERSION_INCREMENTING:
                return true;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }

    public List<Version> getVersions(ByteArray key) {
        return get(key, VERSION_OP, null);
    }

    private final class GetCallable<R> implements Callable<GetResult<R>> {

        private final Node node;
        private final ByteArray key;
        private final StoreOp<R> fetcher;

        public GetCallable(Node node, ByteArray key, StoreOp<R> fetcher) {
            this.node = node;
            this.key = key;
            this.fetcher = fetcher;
        }

        public GetResult<R> call() throws Exception {
            List<R> fetched = Collections.emptyList();
            Throwable exception = null;
            try {
                if(logger.isTraceEnabled())
                    logger.trace("Attempting get operation on node " + node.getId() + " for key '"
                                 + ByteUtils.toHexString(key.get()) + "'.");
                fetched = fetcher.execute(innerStores.get(node.getId()), key);
                node.getStatus().setAvailable();
            } catch(UnreachableStoreException e) {
                exception = e;
                markUnavailable(node, e);
            } catch(Throwable e) {
                if(e instanceof Error)
                    throw (Error) e;
                logger.warn("Error in GET on node " + node.getId() + "(" + node.getHost() + ")", e);
                exception = e;
            }
            return new GetResult<R>(node, key, fetched, exception);
        }
    }

    private final static class GetResult<R> {

        final Node node;
        final ByteArray key;
        final List<R> retrieved;
        final Throwable exception;

        public GetResult(Node node, ByteArray key, List<R> retrieved, Throwable exception) {
            this.node = node;
            this.key = key;
            this.retrieved = retrieved;
            this.exception = exception;
        }

    }

    private final class GetAllCallable implements Callable<GetAllResult> {

        private final Node node;
        private final Collection<ByteArray> nodeKeys;

        private GetAllCallable(Node node, Collection<ByteArray> nodeKeys) {
            this.node = node;
            this.nodeKeys = nodeKeys;
        }

        public GetAllResult call() {
            Map<ByteArray, List<Versioned<byte[]>>> retrieved = Collections.emptyMap();
            Throwable exception = null;
            List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();
            try {
                retrieved = innerStores.get(node.getId()).getAll(nodeKeys);
                if(repairReads) {
                    for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: retrieved.entrySet())
                        fillRepairReadsValues(nodeValues, entry.getKey(), node, entry.getValue());
                    for(ByteArray nodeKey: nodeKeys) {
                        if(!retrieved.containsKey(nodeKey))
                            fillRepairReadsValues(nodeValues,
                                                  nodeKey,
                                                  node,
                                                  Collections.<Versioned<byte[]>> emptyList());
                    }
                }
                node.getStatus().setAvailable();
            } catch(UnreachableStoreException e) {
                exception = e;
                markUnavailable(node, e);
            } catch(Throwable e) {
                if(e instanceof Error)
                    throw (Error) e;
                exception = e;
                logger.warn("Error in GET on node " + node.getId() + "(" + node.getHost() + ")", e);
            }
            return new GetAllResult(this, retrieved, nodeValues, exception);
        }
    }

    private static class GetAllResult {

        final GetAllCallable callable;
        final Map<ByteArray, List<Versioned<byte[]>>> retrieved;
        /* Note that this can never be an Error subclass */
        final Throwable exception;
        final List<NodeValue<ByteArray, byte[]>> nodeValues;

        private GetAllResult(GetAllCallable callable,
                             Map<ByteArray, List<Versioned<byte[]>>> retrieved,
                             List<NodeValue<ByteArray, byte[]>> nodeValues,
                             Throwable exception) {
            this.callable = callable;
            this.exception = exception;
            this.retrieved = retrieved;
            this.nodeValues = nodeValues;
        }
    }

    private interface StoreOp<R> {

        List<R> execute(Store<ByteArray, byte[]> store, ByteArray key);
    }
}

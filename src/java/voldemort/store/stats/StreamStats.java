package voldemort.store.stats;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import voldemort.utils.Time;

import com.google.common.collect.ImmutableList;

public class StreamStats {

    private static final int MAX_ENTRIES = 64;

    private final Map<Long, Handle> handles;
    private final AtomicLong handleIdGenerator;
    private final ConcurrentMap<Operation, RequestCounter> networkCounter;
    private final ConcurrentMap<Operation, RequestCounter> diskCounter;

    public StreamStats() {
        this.handles = Collections.synchronizedMap(new Cache(MAX_ENTRIES));
        this.handleIdGenerator = new AtomicLong(0L);
        this.networkCounter = new ConcurrentHashMap<Operation, RequestCounter>();
        this.diskCounter = new ConcurrentHashMap<Operation, RequestCounter>();

        for(Operation operation: Operation.values()) {
            networkCounter.put(operation, new RequestCounter(300000));
            diskCounter.put(operation, new RequestCounter(30000));
        }
    }

    public Handle makeHandle(Operation operation,
                             HashMap<Integer, List<Integer>> replicaToPartitionList) {
        Handle handle = new Handle(handleIdGenerator.getAndIncrement(),
                                   operation,
                                   System.currentTimeMillis(),
                                   replicaToPartitionList);
        handles.put(handle.getId(), handle);
        return handle;
    }

    public void closeHandle(Handle handle) {
        handle.setFinished(true);
    }

    public void clearFinished() {
        for(long handleId: getHandleIds()) {
            if(getHandle(handleId).isFinished())
                handles.remove(handleId);
        }
    }

    protected Handle getHandle(long handleId) {
        if(!handles.containsKey(handleId))
            throw new IllegalArgumentException("No handle with id " + handleId);

        return handles.get(handleId);
    }

    public Collection<Long> getHandleIds() {
        return ImmutableList.copyOf(handles.keySet());
    }

    public Collection<Handle> getHandles() {
        return ImmutableList.copyOf(handles.values());
    }

    public void recordNetworkTime(Handle handle, long timeNs) {
        networkCounter.get(handle.getOperation()).addRequest(timeNs);
    }

    public void recordDiskTime(Handle handle, long timeNs) {
        diskCounter.get(handle.getOperation()).addRequest(timeNs);
    }

    public RequestCounter getNetworkCounter(Operation operation) {
        return networkCounter.get(operation);
    }

    public RequestCounter getDiskCounter(Operation operation) {
        return diskCounter.get(operation);
    }

    public enum Operation {
        FETCH_KEYS,
        FETCH_ENTRIES,
        FETCH_FILE,
        UPDATE,
        SLOP,
        DELETE,
    }

    private static class Cache extends LinkedHashMap<Long, Handle> {

        private static final long serialVersionUID = 1L;

        private final int maxEntries;

        public Cache(int maxEntries) {
            super();
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Handle> eldest) {
            return eldest.getValue().isFinished() && size() > maxEntries;
        }
    }

    public static class Handle {

        private final long id;
        private final Operation operation;
        private final long startedMs;
        private final HashMap<Integer, List<Integer>> replicaToPartitionList;
        private final AtomicLong entriesScanned;
        private final AtomicLong timeNetworkNs;
        private final AtomicLong timeDiskNs;
        private volatile boolean finished;

        private Handle(long id,
                       Operation operation,
                       long startedMs,
                       HashMap<Integer, List<Integer>> replicaToPartitionList) {
            this.id = id;
            this.operation = operation;
            this.startedMs = startedMs;
            this.replicaToPartitionList = replicaToPartitionList;
            this.entriesScanned = new AtomicLong(0L);
            this.timeNetworkNs = new AtomicLong(0L);
            this.timeDiskNs = new AtomicLong(0L);
            this.finished = false;
        }

        public long getId() {
            return id;
        }

        public long getStartedMs() {
            return startedMs;
        }

        public Operation getOperation() {
            return operation;
        }

        public long getEntriesScanned() {
            return entriesScanned.get();
        }

        public long incrementEntriesScanned() {
            return entriesScanned.incrementAndGet();
        }

        public void setEntriesScanned(long newVal) {
            entriesScanned.set(newVal);
        }

        public long getEntriesPerSecond() {
            long elapsedSecs = System.currentTimeMillis() - startedMs;
            if(elapsedSecs == 0L)
                return 0L;
            return getEntriesScanned() / elapsedSecs;
        }

        public boolean isFinished() {
            return finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        public HashMap<Integer, List<Integer>> getReplicaToPartitionList() {
            return replicaToPartitionList;
        }

        public void recordTimeNetwork(long deltaNs) {
            timeNetworkNs.addAndGet(deltaNs);
        }

        public long getTimeNetworkNs() {
            return timeNetworkNs.get();
        }

        public void recordTimeDisk(long deltaNs) {
            timeDiskNs.addAndGet(deltaNs);
        }

        public long getTimeDiskNs() {
            return timeDiskNs.get();
        }

        public double getPercentDisk() {
            long timeDiskMs = getTimeDiskNs() / Time.NS_PER_MS;
            return (timeDiskMs * 100.0) / (System.currentTimeMillis() - startedMs);
        }

        public double getPercentNetwork() {
            long timeNetworkMs = getTimeNetworkNs() / Time.NS_PER_MS;
            return (timeNetworkMs * 100.0) / (System.currentTimeMillis() - startedMs);
        }

        @Override
        public String toString() {
            return "Handle{" + "id=" + id + ", operation=" + operation + ", startedMs=" + startedMs
                   + ", replicaToPartitionList=" + getReplicaToPartitionList()
                   + ", entriesScanned=" + getEntriesScanned() + ", finished=" + finished
                   + ", entriesPerSecond=" + getEntriesPerSecond() + ", timeDiskNs="
                   + getTimeDiskNs() + ", timeNetworkNs=" + getTimeNetworkNs() + ", percentDisk="
                   + getPercentDisk() + ", percentNetwork=" + getPercentNetwork() + '}';
        }
    }
}

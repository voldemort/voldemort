package voldemort.store.stats;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamStats {

    private final ConcurrentMap<Long, Handle> handles;
    private final AtomicLong handleIdGenerator;

    public StreamStats () {
        this.handles = new ConcurrentHashMap<Long, Handle>();
        this.handleIdGenerator = new AtomicLong(0L);
    }

    public Handle makeHandle(Operation operation, List<Integer> partitionIds) {
        Handle handle = new Handle(handleIdGenerator.getAndIncrement(),
                                   operation,
                                   System.currentTimeMillis(),
                                   partitionIds);
        handles.put(handle.getId(), handle);
        return handle;
    }

    public void closeHandle(Handle handle) {
        handles.remove(handle.getId());
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

    public enum Operation {
        FETCH_KEYS,
        FETCH_ENTRIES,
        FETCH_FILE,
        UPDATE,
        SLOP,
        DELETE,
    }

    public static class Handle {

        private final long id;
        private final Operation operation;
        private final long startedMs;
        private final List<Integer> partitionIds;
        private final AtomicLong entriesScanned;
        private final AtomicLong timeNetwork;
        private final AtomicLong timeDisk;
        private volatile boolean finished;

        private Handle(long id, Operation operation, long startedMs, List<Integer> partitionIds) {
            this.id = id;
            this.operation = operation;
            this.startedMs = startedMs;
            this.partitionIds = partitionIds;
            this.entriesScanned = new AtomicLong(0L);
            this.timeNetwork = new AtomicLong(0L);
            this.timeDisk = new AtomicLong(0L);
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

        public List<Integer> getPartitionIds() {
            return partitionIds;
        }

        public void recordTimeNetwork(long deltaMs) {
            timeNetwork.addAndGet(deltaMs);
        }

        public long getTimeNetwork() {
            return timeNetwork.get();
        }

        public void recordTimeDisk(long deltaMs) {
            timeDisk.addAndGet(deltaMs);
        }

        public long getTimeDisk() {
            return timeDisk.get();
        }

        @Override
        public String toString() {
            return "Handle{" +
                   "id=" + id +
                   ", operation=" + operation +
                   ", startedMs=" + startedMs +
                   ", partitionIds=" + partitionIds +
                   ", entriesScanned=" + getEntriesScanned() +
                   ", finished=" + finished +
                   ", entriesPerSecond=" + getEntriesPerSecond() +
                   '}';
        }
    }
}

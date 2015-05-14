package voldemort.server.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ScanPermitWrapper {

    private final Semaphore scanPermits;
    private Map<String, AtomicLong> permitOwnersScanProgressMap;
    private Map<String, AtomicLong> permitOwnersDeleteProgressMap;
    private final int numPermits;

    private long totalEntriesScanned;
    private long totalEntriesDeleted;

    public ScanPermitWrapper(final int numPermits) {
        this.numPermits = numPermits;
        scanPermits = new Semaphore(numPermits);
        permitOwnersScanProgressMap = Collections.synchronizedMap(new HashMap<String, AtomicLong>());
        permitOwnersDeleteProgressMap = Collections.synchronizedMap(new HashMap<String, AtomicLong>());
        this.totalEntriesScanned = 0;
        this.totalEntriesDeleted = 0;
    }

    public void acquire(AtomicLong progress, String ownerName) throws InterruptedException {
        this.scanPermits.acquire();
        synchronized(permitOwnersScanProgressMap) {
            permitOwnersScanProgressMap.put(ownerName, progress);
        }
    }

    private void initializeProgressMaps(AtomicLong scanProgress,
                                        AtomicLong deleteProgress,
                                        String ownerName) {
        synchronized(permitOwnersScanProgressMap) {
            permitOwnersScanProgressMap.put(ownerName, scanProgress);
        }

        synchronized(permitOwnersDeleteProgressMap) {
            permitOwnersDeleteProgressMap.put(ownerName, deleteProgress);
        }
    }

    public void acquire(AtomicLong scanProgress, AtomicLong deleteProgress, String ownerName)
            throws InterruptedException {
        this.scanPermits.acquire();
        initializeProgressMaps(scanProgress, deleteProgress, ownerName);
    }

    public void release(String ownerName) {
        this.scanPermits.release();
        synchronized(permitOwnersScanProgressMap) {
            AtomicLong scannedCount = permitOwnersScanProgressMap.get(ownerName);
            if(scannedCount != null)
                totalEntriesScanned += scannedCount.get();
            permitOwnersScanProgressMap.remove(ownerName);
        }

        synchronized(permitOwnersDeleteProgressMap) {
            AtomicLong deletedCount = permitOwnersDeleteProgressMap.get(ownerName);
            if(deletedCount != null)
                totalEntriesDeleted += deletedCount.get();
            permitOwnersDeleteProgressMap.remove(ownerName);
        }
    }

    public List<String> getPermitOwners() {
        List<String> ownerList = new ArrayList<String>();
        synchronized(permitOwnersScanProgressMap) {
            Iterator<String> i = this.permitOwnersScanProgressMap.keySet().iterator();
            while(i.hasNext())
                ownerList.add(i.next());
        }
        return ownerList;
    }

    public boolean tryAcquire(AtomicLong scanProgress, AtomicLong deleteProgress, String ownerName) {
        boolean gotPermit = this.scanPermits.tryAcquire();
        if(gotPermit) {
            initializeProgressMaps(scanProgress, deleteProgress, ownerName);
        }
        return gotPermit;
    }

    public int availablePermits() {
        return this.scanPermits.availablePermits();
    }

    public int getGrantedPermits() {
        return numPermits - availablePermits();
    }

    public long getEntriesScanned() {
        long itemsScanned = 0;
        synchronized(permitOwnersScanProgressMap) {
            for(Map.Entry<String, AtomicLong> progressEntry: permitOwnersScanProgressMap.entrySet()) {
                AtomicLong progress = progressEntry.getValue();
                // slops are not included since they are tracked separately
                if(progress != null) {
                    itemsScanned += progress.get();
                }
            }
        }
        return totalEntriesScanned + itemsScanned;
    }

    public long getEntriesDeleted() {
        long itemsDeleted = 0;
        synchronized(permitOwnersDeleteProgressMap) {
            for(Map.Entry<String, AtomicLong> progressEntry: permitOwnersDeleteProgressMap.entrySet()) {
                AtomicLong progress = progressEntry.getValue();
                // slops are not included since they are tracked separately
                if(progress != null) {
                    itemsDeleted += progress.get();
                }
            }
        }
        return totalEntriesDeleted + itemsDeleted;
    }
}

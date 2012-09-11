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
    private Map<String, AtomicLong> permitOwners;
    private final int numPermits;

    private long totalEntriesScanned;

    public ScanPermitWrapper(final int numPermits) {
        this.numPermits = numPermits;
        scanPermits = new Semaphore(numPermits);
        permitOwners = Collections.synchronizedMap(new HashMap<String, AtomicLong>());
    }

    public static String getOwnerName() {
        return Thread.currentThread().getStackTrace()[2].getClassName();
    }

    public void acquire(AtomicLong progress) throws InterruptedException {
        this.scanPermits.acquire();
        synchronized(permitOwners) {
            permitOwners.put(getOwnerName(), progress);
        }
    }

    public void release() {
        this.scanPermits.release();
        synchronized(permitOwners) {
            AtomicLong scannedCount = permitOwners.get(getOwnerName());
            if(scannedCount != null)
                totalEntriesScanned += scannedCount.get();
            permitOwners.remove(getOwnerName());
        }
    }

    public List<String> getPermitOwners() {
        List<String> ownerList = new ArrayList<String>();
        synchronized(permitOwners) {
            Iterator<String> i = this.permitOwners.keySet().iterator();
            while(i.hasNext())
                ownerList.add(i.next());
        }
        return ownerList;
    }

    public boolean tryAcquire(AtomicLong progress) {
        boolean gotPermit = this.scanPermits.tryAcquire();
        if(gotPermit) {
            synchronized(permitOwners) {
                permitOwners.put(getOwnerName(), progress);
            }
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
        synchronized(permitOwners) {
            for(Map.Entry<String, AtomicLong> progressEntry: permitOwners.entrySet()) {
                AtomicLong progress = progressEntry.getValue();
                // slops are not included since they are tracked separately
                if(progress != null) {
                    itemsScanned += progress.get();
                }
            }
        }
        return totalEntriesScanned + itemsScanned;
    }
}

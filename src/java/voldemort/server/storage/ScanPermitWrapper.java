package voldemort.server.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ScanPermitWrapper {

    private final Semaphore scanPermits;
    private List<String> permitOwners;

    public ScanPermitWrapper(final int numPermits) {
        scanPermits = new Semaphore(numPermits);
        permitOwners = Collections.synchronizedList(new ArrayList<String>());
    }

    public void acquire() throws InterruptedException {
        this.scanPermits.acquire();
        synchronized(permitOwners) {
            permitOwners.add(Thread.currentThread().getStackTrace()[2].getClassName());
        }
    }

    public void release() {
        this.scanPermits.release();
        synchronized(permitOwners) {
            permitOwners.remove(Thread.currentThread().getStackTrace()[2].getClassName());
        }
    }

    public List<String> getPermitOwners() {
        List<String> ownerList = new ArrayList<String>();
        synchronized(permitOwners) {
            Iterator<String> i = this.permitOwners.iterator();
            while(i.hasNext())
                ownerList.add(i.next());
        }
        return ownerList;
    }

    public boolean tryAcquire() {
        boolean gotPermit = this.scanPermits.tryAcquire();
        if(gotPermit) {
            synchronized(permitOwners) {
                permitOwners.add(Thread.currentThread().getStackTrace()[2].getClassName());
            }
        }
        return gotPermit;
    }

    public int availablePermits() {
        return this.scanPermits.availablePermits();
    }
}

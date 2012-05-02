package voldemort.server.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import voldemort.annotations.jmx.JmxManaged;

@JmxManaged(description = "Wrapper for Scan permit.")
public class ScanPermitWrapper {

    private final Semaphore scanPermits;
    private List<String> permitOwners;

    public ScanPermitWrapper(int numPermits) {
        scanPermits = new Semaphore(numPermits);
        permitOwners = new ArrayList<String>();
    }

    public synchronized void acquire() throws InterruptedException {
        this.scanPermits.acquire();
        permitOwners.add(Thread.currentThread().getStackTrace()[2].getClassName());
    }

    public synchronized void release() {
        this.scanPermits.release();
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        permitOwners.remove(className);
    }

    public List<String> getPermitOwners() {
        return this.permitOwners;
    }

    public synchronized boolean tryAcquire() {
        boolean gotPermit = this.scanPermits.tryAcquire();
        if(gotPermit)
            permitOwners.add(Thread.currentThread().getStackTrace()[2].getClassName());
        return gotPermit;
    }

    public synchronized int availablePermits() {
        return this.scanPermits.availablePermits();
    }
}

package voldemort.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A parallel keyed lock
 * 
 * @author jay
 * 
 */
public class StripedLock {

    private static final FnvHashFunction hash = new FnvHashFunction();

    private final Lock[] locks;

    public StripedLock(int locks) {
        this.locks = new ReentrantLock[locks];
        for(int i = 0; i < this.locks.length; i++)
            this.locks[i] = new ReentrantLock();
    }

    public Lock lockFor(int key) {
        return locks[Math.abs(key % locks.length)];
    }

    public Lock lockFor(long key) {
        int k = (int) (key % Integer.MAX_VALUE);
        return lockFor(k);
    }

    public Lock lockFor(byte[] key) {
        return lockFor(hash.hash(key));
    }
}

package voldemort.utils;

/**
 * A parallel keyed lock
 * 
 * @author jay
 * 
 */
public class StripedLock {

    private static final FnvHashFunction hash = new FnvHashFunction();

    private final Object[] locks;

    public StripedLock(int locks) {
        this.locks = new Object[locks];
        for(int i = 0; i < this.locks.length; i++)
            this.locks[i] = new Object();
    }

    public Object lockFor(int key) {
        return locks[Math.abs(key % locks.length)];
    }

    public Object lockFor(long key) {
        int k = (int) (key % Integer.MAX_VALUE);
        return lockFor(k);
    }

    public Object lockFor(byte[] key) {
        return lockFor(hash.hash(key));
    }
}

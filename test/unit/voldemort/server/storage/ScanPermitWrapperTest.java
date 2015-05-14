package voldemort.server.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

/**
 * JUnit test cases to test ScanPermitWrapper functionality
 * 
 */
public class ScanPermitWrapperTest {

    /**
     * Test that the owner is set properly
     */
    @Test
    public void testBasicPermitOwner() {
        ScanPermitWrapper wrapper = new ScanPermitWrapper(1);
        try {
            wrapper.acquire(null, this.getClass().getCanonicalName());
        } catch(InterruptedException e) {
            fail("Failure to acquire the Scan permit: " + e);
        }

        try {
            List<String> ownerList = wrapper.getPermitOwners();
            assertEquals(ownerList.size(), 1);
            assertEquals(ownerList.get(0), this.getClass().getCanonicalName());

            wrapper.release(this.getClass().getCanonicalName());
        } catch(Exception e) {
            fail("Failure to get correct owner list : " + e);
        }
    }

    /**
     * Validate that progress is tracked correctly
     */
    @Test
    public void testProgress() {
        ScanPermitWrapper wrapper = new ScanPermitWrapper(1);
        AtomicLong scanProgress = new AtomicLong(0);
        AtomicLong deleteProgress = new AtomicLong(0);

        try {
            wrapper.acquire(scanProgress, deleteProgress, this.getClass().getCanonicalName());
        } catch(InterruptedException e) {
            fail("Failure to acquire the Scan permit: " + e);
        }

        try {
            scanProgress.incrementAndGet();
            scanProgress.incrementAndGet();
            scanProgress.incrementAndGet();
            scanProgress.incrementAndGet();
            scanProgress.incrementAndGet();

            deleteProgress.incrementAndGet();
            deleteProgress.incrementAndGet();
            deleteProgress.incrementAndGet();

            wrapper.release(this.getClass().getCanonicalName());
        } catch(Exception e) {
            fail("Unexpected failure occurred : " + e);
        }

        assertEquals(5, wrapper.getEntriesScanned());
        assertEquals(3, wrapper.getEntriesDeleted());
    }

    /**
     * Validate that aggregate progress is tracked correctly
     */
    @Test
    public void testProgressInMultipleIterations() {
        ScanPermitWrapper wrapper = new ScanPermitWrapper(1);

        for(int i = 0; i < 3; i++) {
            System.out.println("Running iteration " + i);
            AtomicLong scanProgress = new AtomicLong(0);
            AtomicLong deleteProgress = new AtomicLong(0);

            try {
                wrapper.acquire(scanProgress, deleteProgress, this.getClass().getCanonicalName());

                scanProgress.incrementAndGet();
                scanProgress.incrementAndGet();
                scanProgress.incrementAndGet();
                scanProgress.incrementAndGet();
                scanProgress.incrementAndGet();

                deleteProgress.incrementAndGet();
                deleteProgress.incrementAndGet();
                deleteProgress.incrementAndGet();

                wrapper.release(this.getClass().getCanonicalName());
            } catch(InterruptedException e) {
                fail("Failure to acquire the Scan permit: " + e);
            } catch(Exception e) {
                fail("Unexpected failure occurred : " + e);
            }
        }

        assertEquals(15, wrapper.getEntriesScanned());
        assertEquals(9, wrapper.getEntriesDeleted());

    }
}

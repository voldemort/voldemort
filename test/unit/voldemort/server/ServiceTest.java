package voldemort.server;

import junit.framework.TestCase;

/**
 * @author jay
 *
 */
public class ServiceTest extends TestCase {
    
    public void testBeginsUnstarted() {
        FakeService s = new FakeService();
        assertFalse(s.isStarted());
        assertFalse(s.isStarted());
    }
    
    public void testStartStartsAndStopStops() {
        FakeService s = new FakeService();
        s.start();
        assertTrue(s.isStarted());
        s.stop();
        assertFalse(s.isStarted());
        s.start();
        assertTrue(s.isStarted());
    }
    
    public void testStartupFailureLeavesUnstarted() {
        FakeService s = new FakeService(new RuntimeException("FAIL!"), null);
        try {
            s.start();
        } catch(RuntimeException e) {
            assertFalse(s.isStarted());
        }
    }
    
    public void testShutdownFailureLeavesUnstarted() {
        FakeService s = new FakeService(null, new RuntimeException("FAIL!"));
        s.start();
        try {
            s.stop();
        } catch(RuntimeException e) {
            assertFalse(s.isStarted());
        }
    }
    
    private static class FakeService extends AbstractService {
        
        private RuntimeException startException = null;
        private RuntimeException stopException = null;
        
        public FakeService() {
            super("do-nothing");
        }
        
        public FakeService(RuntimeException startException, RuntimeException stopException) {
            super("do-nothing");
            this.startException = startException;
            this.stopException = stopException;
        }

        @Override
        protected void startInner() {
            if(startException != null)
                throw startException;
        }

        @Override
        protected void stopInner() {
            if(stopException != null)
                throw stopException;
        }
        
    }
    
}

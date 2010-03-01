/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server;

import junit.framework.TestCase;

/**
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

    private static class FakeService extends AbstractService {

        private RuntimeException startException = null;
        private RuntimeException stopException = null;

        public FakeService() {
            super(ServiceType.SCHEDULER);
        }

        public FakeService(RuntimeException startException, RuntimeException stopException) {
            super(ServiceType.SCHEDULER);
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

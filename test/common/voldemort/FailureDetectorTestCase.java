/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort;

import voldemort.cluster.failuredetector.FailureDetector;

/**
 * An interface that is used to denote when a unit test uses a FailureDetector
 * internally for the test. Rather than using one specific implementation we
 * create a TestSuite to execute the test with all known implementations.
 * 
 * <p/>
 * 
 * <b>Note</b>: this interface should be implemented by a TestCase class. The
 * TestClass implementation should have an instance variable that stores the
 * argument value and uses it appropriately.
 * 
 * Here's an example:
 * 
 * <pre>
 * 
 * public class MyTestCase extends TestCase implements FailureDetectorTestCase {
 * 
 *     private FailureDetector failureDetector;
 * 
 *     private Class&lt;FailureDetector&gt; failureDetectorClass;
 * 
 *     public static Test suite() {
 *         return TestUtils.createFailureDetectorTestSuite(MyTestCase.class);
 *     }
 * 
 *     public void setFailureDetectorClass(Class&lt;FailureDetector&gt; failureDetectorClass) {
 *         this.failureDetectorClass = failureDetectorClass;
 *     }
 * 
 *     &#064;Override
 *     public void setUp() throws Exception {
 *         super.setUp();
 * 
 *         Map&lt;Integer, Store&lt;ByteArray, byte[]&gt;&gt; stores = . . .
 * 
 *         failureDetector = failureDetectorClass.newInstance(); 
 *         failureDetector.setNodeBannageMs(10000);
 *         failureDetector.setStores(subStores);
 *     }
 * 
 *     public void testSomething() throws Exception {
 *         . . .
 *     }
 * 
 * }
 * </pre>
 * 
 * @author Kirk True
 * 
 * @see TestUtils#createFailureDetectorTestSuite
 */

public interface FailureDetectorTestCase {

    /**
     * Executed as the TestCase is put into the TestSuite. The value here should
     * be used to construct a new FailureDetector for testing.
     * 
     * @param failureDetectorClass Implementation of FailureDetector to use
     */

    public void setFailureDetectorClass(Class<FailureDetector> failureDetectorClass);

}

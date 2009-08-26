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

/**
 * An interface that is used to denote when a unit test uses a SocketService
 * internally for the test. Rather than using one specific implementation (BIO
 * or NIO), we create a TestSuite to execute both types.
 * 
 * <p/>
 * 
 * <b>Note</b>: this interface should be implemented by a TestCase class. The
 * TestClass implementation should have an instance variable that stores the
 * argument value and passes it to the ServerTestUtils' getSocketService method.
 * 
 * Here's an example usage:
 * 
 * <pre>
 * public class MyTestCase extends TestCase implements SocketServiceTestCase {
 * 
 *     private AbstractSocketService socketService;
 * 
 *     private boolean useNio;
 * 
 *     public static Test suite() {
 *         return TestUtils.createSocketServiceTestCaseSuite(MyTestCase.class);
 *     }
 * 
 *     public void setUseNio(boolean useNio) {
 *         this.useNio = useNio;
 *     }
 * 
 *     &#064;Override
 *     public void setUp() throws Exception {
 *         super.setUp();
 *         socketService = ServerTestUtils.getSocketService(useNio, . . .);
 *         socketService.start();
 *     }
 * 
 *     &#064;Override
 *     public void tearDown() throws Exception {
 *         super.tearDown();
 *         socketService.stop();
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
 * @see TestUtils#createSocketServiceTestCaseSuite
 * @see ServerTestUtils#getSocketService
 */

public interface SocketServiceTestCase {

    /**
     * Executed as the TestCase is put into the TestSuite. The value here should
     * be passed into the ServerTestUtils.getSocketService method.
     * 
     * @param useNio True to use the NIO implementation of the SocketService,
     *        false to use the BIO implementation
     */

    public void setUseNio(boolean useNio);

}

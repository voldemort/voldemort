/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils;

/**
 * RemoteTest represents the operation of invoking the voldemort-remote-test.sh
 * script on remote machines in parallel.
 * 
 * <p/>
 * 
 * The remote tests are executed in parallel against the set of server nodes
 * returned by the bootstrap Voldemort server's meta data.
 * 
 * <p/>
 * 
 * A list of RemoteTestResult instances is returned, one for each client. The
 * result includes the values for the reads/second, writes/second, and
 * deletes/second for each iteration. These raw numbers can then be manipulated
 * as seen fit for the desired measurement.
 * 
 * <p/>
 * 
 * Implementation notes:
 * 
 * <ol>
 * <li>Implementations must provide a reasonable guarantee that the clients were
 * actually started. An error should be raised, therefore, if the server could
 * not be started, regardless of cause.
 * <li>It is assumed that the remote host is properly set up to start the
 * Voldemort test. This means that:
 * <ul>
 * <li>The Java environment is properly configured with $JAVA_HOME pointing at a
 * valid JDK.
 * <li>The Voldemort distribution is already present at a known location on the
 * remote host.
 * </ul>
 * <li>The test run should occur in parallel against the remote hosts, if
 * possible.
 * </ol>
 * 
 * @author Kirk True
 * 
 * @see RemoteTestResult
 */

public interface RemoteTest extends RemoteOperation {

}
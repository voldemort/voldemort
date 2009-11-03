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
 * Deployer represents the operation of remotely deploying the Voldemort
 * distribution to a set of Voldemort clients and/or servers. A <i>Voldemort
 * distribution</i> is the set of binaries, configuration files, etc. that
 * comprise the Voldemort system. For example, the binary distribution (.zip,
 * .tar.gz, etc.) of Voldemort includes a top-level directory of the name
 * <i>voldemort-&lt;version&gt;</i>, under which are directories such as
 * <i>bin</i>, <i>config</i>, <i>dist</i>, etc. It is this top-level directory
 * (<i>voldemort-&lt;version&gt;</i>) that is deployed.
 * 
 * <p/>
 * 
 * Implementation notes:
 * 
 * <ol>
 * <li>Implementations must provide a reasonable guarantee that the deployment
 * actually occurred. An error should be raised, therefore, if the data could
 * not be copied, regardless of cause.
 * <li>No precaution or assumption need be made as to the state of the data
 * being copied. For example, no checks are made to ensure that a Voldemort
 * server is not currently running on the given remote host, using this data
 * directory upon which this operation is being performed.
 * </ol>
 * 
 * @author Kirk True
 */

public interface Deployer extends RemoteOperation {

}
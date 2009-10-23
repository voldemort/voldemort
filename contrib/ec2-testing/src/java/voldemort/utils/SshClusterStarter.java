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

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class SshClusterStarter extends CommandLineAction implements ClusterStarter {

    public void start(Collection<String> hostNames,
                      String hostUserId,
                      File sshPrivateKey,
                      String voldemortRootDirectory,
                      String voldemortHomeDirectory,
                      long timeout) throws StartClusterException {
        StringBuilder errors = new StringBuilder();

        try {
            run("SshClusterStarter.ssh",
                hostNames,
                hostUserId,
                sshPrivateKey,
                voldemortRootDirectory,
                voldemortHomeDirectory,
                null,
                timeout,
                errors);
        } catch(IOException e) {
            throw new StartClusterException(e);
        }

        if(errors.length() > 0)
            throw new StartClusterException(errors.toString());
    }

}

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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class SshClusterStopper extends CommandLineClusterOperation<Object> implements
        ClusterOperation<Object> {

    private final AtomicInteger completedCounter = new AtomicInteger();

    private final int hostCount;

    private final CommandOutputListener outputListener = new SshClusterStopperCommandOutputListener();

    public SshClusterStopper(CommandLineClusterConfig commandLineClusterConfig) {
        super(commandLineClusterConfig, "SshClusterStopper.ssh");
        hostCount = commandLineClusterConfig.getHostNames().size();
    }

    @Override
    public List<Object> execute() throws ClusterOperationException {
        if(logger.isInfoEnabled())
            logger.info("Stopping Voldemort cluster");

        return super.execute();
    }

    @Override
    protected Callable<Object> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(outputListener,
                                                                                       logger);
        return new ExitCodeCallable<Object>(command, commandOutputListener);
    }

    public class SshClusterStopperCommandOutputListener implements CommandOutputListener {

        public void outputReceived(String hostName, String line) {
            if(line.contains("Stopping Voldemort...")) {
                completedCounter.incrementAndGet();

                if(logger.isInfoEnabled()) {
                    logger.info(hostName + " shutdown complete");

                    if(hostCount == completedCounter.get())
                        logger.info("Cluster shutdown complete");
                }
            }
        }
    }

}

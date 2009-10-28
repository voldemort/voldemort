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

package voldemort.utils.impl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.utils.ClusterOperation;
import voldemort.utils.ClusterOperationException;

public class SshClusterStarter extends CommandLineClusterOperation<Object> implements
        ClusterOperation<Object> {

    private final AtomicInteger completedCounter = new AtomicInteger();

    private final int hostCount;

    private final CommandOutputListener outputListener = new SshClusterStarterCommandOutputListener();

    public SshClusterStarter(CommandLineClusterConfig commandLineClusterConfig) {
        super(commandLineClusterConfig, "SshClusterStarter.ssh");
        hostCount = commandLineClusterConfig.getHostNames().size();
    }

    @Override
    public List<Object> execute() throws ClusterOperationException {
        if(logger.isInfoEnabled())
            logger.info("Starting Voldemort cluster");

        return super.execute();
    }

    @Override
    protected Callable<Object> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(outputListener,
                                                                                       logger);
        return new ClusterStarterCallable<Object>(command, commandOutputListener);
    }

    public class SshClusterStarterCommandOutputListener implements CommandOutputListener {

        public void outputReceived(String hostName, String line) {
            if(line.contains("Startup completed")) {
                completedCounter.incrementAndGet();

                if(logger.isInfoEnabled()) {
                    logger.info(hostName + " startup complete");

                    if(hasStartupCompleted())
                        logger.info("Cluster startup complete");
                }
            }
        }
    }

    private boolean hasStartupCompleted() {
        return hostCount == completedCounter.get();
    }

    private class ClusterStarterCallable<T> implements Callable<T> {

        private final UnixCommand command;

        private final CommandOutputListener commandOutputListener;

        public ClusterStarterCallable(UnixCommand command,
                                      CommandOutputListener commandOutputListener) {
            this.command = command;
            this.commandOutputListener = commandOutputListener;
        }

        public T call() throws Exception {
            int exitCode = command.execute(commandOutputListener);

            // If the user hits Ctrl+C after startup, we get an exit code of
            // 255, so don't throw an exception in this case.
            if(!(exitCode == 255 && hasStartupCompleted()))
                throw new Exception("Process on " + command.getHostName() + " exited with code "
                                    + exitCode + ". Please check the logs for details.");

            return null;
        }
    }

}

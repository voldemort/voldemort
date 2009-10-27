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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;

public class SshRemoteTest extends CommandLineClusterOperation<RemoteTestResult> implements
        ClusterOperation<RemoteTestResult> {

    public SshRemoteTest(CommandLineClusterConfig commandLineClusterConfig) {
        super(commandLineClusterConfig, "SshRemoteTest.ssh");
    }

    @Override
    protected Callable<RemoteTestResult> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(null);
        IterationTrackerCommandOutputListener iterationTrackerCommandOutputListener = new IterationTrackerCommandOutputListener(commandOutputListener);
        return new RemoteTestResultCallable(command, iterationTrackerCommandOutputListener);
    }

    protected class IterationTrackerCommandOutputListener extends DelegatingCommandOutputListener {

        private final RemoteTestOutputParser remoteTestOutputParser;

        public IterationTrackerCommandOutputListener(CommandOutputListener delegate) {
            super(delegate);
            this.remoteTestOutputParser = new RemoteTestOutputParser();
        }

        public Map<Integer, RemoteTestIteration> getRemoteTestIterations() {
            return remoteTestOutputParser.getRemoteTestIterations();
        }

        @Override
        public void outputReceived(String hostName, String line) {
            remoteTestOutputParser.outputReceived(line);

            super.outputReceived(hostName, line);
        }

    }

    protected class RemoteTestResultCallable implements Callable<RemoteTestResult> {

        private final UnixCommand command;

        private final IterationTrackerCommandOutputListener commandOutputListener;

        public RemoteTestResultCallable(UnixCommand command,
                                        IterationTrackerCommandOutputListener commandOutputListener) {
            this.command = command;
            this.commandOutputListener = commandOutputListener;
        }

        public RemoteTestResult call() throws Exception {
            command.execute(commandOutputListener);

            Map<Integer, RemoteTestIteration> remoteTestIterations = commandOutputListener.getRemoteTestIterations();
            RemoteTestResult remoteTestResult = new RemoteTestResult();
            remoteTestResult.setHostName(command.getHostName());
            remoteTestResult.setRemoteTestIterations(new ArrayList<RemoteTestIteration>(remoteTestIterations.values()));
            return remoteTestResult;
        }

    }

}

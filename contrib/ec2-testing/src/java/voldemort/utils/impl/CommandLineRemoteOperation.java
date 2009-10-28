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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import voldemort.utils.RemoteOperationException;

abstract class CommandLineRemoteOperation<T> {

    protected final Log logger = LogFactory.getLog(getClass());

    protected List<T> execute(Map<String, String> hostNameCommandLineMap)
            throws RemoteOperationException {
        CommandLineParser commandLineParser = new CommandLineParser();

        ExecutorService threadPool = Executors.newFixedThreadPool(hostNameCommandLineMap.size());
        List<Future<T>> futures = new ArrayList<Future<T>>();

        for(Map.Entry<String, String> entry: hostNameCommandLineMap.entrySet()) {
            String hostName = entry.getKey();
            String commandLine = entry.getValue();

            if(logger.isDebugEnabled())
                logger.debug("Command to execute: " + commandLine);

            List<String> commandArgs = commandLineParser.parse(commandLine);
            UnixCommand command = new UnixCommand(hostName, commandArgs);
            Callable<T> callable = getCallable(command);
            Future<T> future = threadPool.submit(callable);
            futures.add(future);
        }

        List<T> list = new ArrayList<T>();

        try {
            StringBuilder errors = new StringBuilder();

            for(Future<T> future: futures) {
                Throwable t = null;

                try {
                    T result = future.get();
                    list.add(result);
                } catch(ExecutionException ex) {
                    t = ex.getCause();
                } catch(Exception e) {
                    t = e;
                }

                if(t != null) {
                    if(logger.isWarnEnabled())
                        logger.warn(t, t);

                    if(errors.length() > 0)
                        errors.append("; ");

                    errors.append(t.getMessage());
                }
            }

            if(errors.length() > 0)
                throw new RemoteOperationException(errors.toString());
        } finally {
            threadPool.shutdown();

            try {
                threadPool.awaitTermination(60, TimeUnit.SECONDS);
            } catch(InterruptedException e) {
                if(logger.isWarnEnabled())
                    logger.warn(e, e);
            }
        }

        return list;
    }

    protected Callable<T> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(null, logger);
        return new ExitCodeCallable<T>(command, commandOutputListener);
    }

}

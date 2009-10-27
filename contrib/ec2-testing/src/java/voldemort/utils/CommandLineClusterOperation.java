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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract class CommandLineClusterOperation<T> {

    protected final CommandLineClusterConfig commandLineClusterConfig;

    protected final String commandId;

    protected final Log logger = LogFactory.getLog(getClass());

    protected CommandLineClusterOperation(CommandLineClusterConfig commandLineClusterConfig,
                                          String commandId) {
        this.commandLineClusterConfig = commandLineClusterConfig;
        this.commandId = commandId;
    }

    public List<T> execute() throws ClusterOperationException {
        Properties properties = new Properties();

        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("commands.properties"));
        } catch(IOException e1) {
            throw new ClusterOperationException(e1);
        }

        final String rawCommand = properties.getProperty(commandId);
        final ExecutorService threadPool = Executors.newFixedThreadPool(commandLineClusterConfig.getHostNames()
                                                                                                .size());
        final List<Future<T>> futures = new ArrayList<Future<T>>();

        for(String hostName: commandLineClusterConfig.getHostNames()) {
            String parameterizedCommand = parameterizeCommand(hostName, rawCommand);
            List<String> commandArgs = generateCommandArgs(parameterizedCommand);
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
                throw new ClusterOperationException(errors.toString());
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
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(null);
        return new ExitCodeCallable<T>(command, commandOutputListener);
    }

    private String parameterizeCommand(String hostName, String command) {
        Map<String, String> variableMap = new HashMap<String, String>();
        variableMap.put("hostName", hostName);
        variableMap.put("hostUserId", commandLineClusterConfig.getHostUserId());

        if(commandLineClusterConfig.getSshPrivateKey() != null)
            variableMap.put("sshPrivateKey", commandLineClusterConfig.getSshPrivateKey()
                                                                     .getAbsolutePath());

        variableMap.put("voldemortParentDirectory",
                        commandLineClusterConfig.getVoldemortParentDirectory());
        variableMap.put("voldemortRootDirectory",
                        commandLineClusterConfig.getVoldemortRootDirectory());
        variableMap.put("voldemortHomeDirectory",
                        commandLineClusterConfig.getVoldemortHomeDirectory());

        // Null-safe access would be nice here ;)
        String nodeId = commandLineClusterConfig.getNodeIds() != null
                        && commandLineClusterConfig.getNodeIds().get(hostName) != null ? commandLineClusterConfig.getNodeIds()
                                                                                                                 .get(hostName)
                                                                                                                 .toString()
                                                                                      : null;

        variableMap.put("voldemortNodeId", nodeId);

        String remoteTestArguments = commandLineClusterConfig.getRemoteTestArguments() != null ? commandLineClusterConfig.getRemoteTestArguments()
                                                                                                                         .get(hostName)
                                                                                              : null;

        variableMap.put("remoteTestArguments", remoteTestArguments);

        if(commandLineClusterConfig.getSourceDirectory() != null)
            variableMap.put("sourceDirectory", commandLineClusterConfig.getSourceDirectory()
                                                                       .getAbsolutePath());

        for(Map.Entry<String, String> entry: variableMap.entrySet())
            command = StringUtils.replace(command, "${" + entry.getKey() + "}", entry.getValue());

        return command;
    }

    private List<String> generateCommandArgs(String command) {
        List<String> commands = new ArrayList<String>();
        boolean isInQuotes = false;
        int start = 0;

        for(int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);

            if(c == '\"') {
                isInQuotes = !isInQuotes;
            } else if(c == ' ' && !isInQuotes) {
                String substring = command.substring(start, i).trim();
                start = i + 1;

                if(substring.trim().length() > 0)
                    commands.add(substring.replace("\"", ""));
            }
        }

        String substring = command.substring(start).trim();

        if(substring.length() > 0)
            commands.add(substring.replace("\"", ""));

        if(logger.isDebugEnabled())
            logger.debug("Command to execute: " + commands.toString());

        return commands;
    }

}

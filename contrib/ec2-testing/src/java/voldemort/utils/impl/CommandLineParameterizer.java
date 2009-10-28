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

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

public class CommandLineParameterizer {

    public static final String HOST_NAME_PARAM = "hostName";
    public static final String HOST_USER_ID_PARAM = "hostUserId";
    public static final String SSH_PRIVATE_KEY_PARAM = "sshPrivateKey";
    public static final String VOLDEMORT_PARENT_DIRECTORY_PARAM = "voldemortParentDirectory";
    public static final String VOLDEMORT_ROOT_DIRECTORY_PARAM = "voldemortRootDirectory";
    public static final String VOLDEMORT_HOME_DIRECTORY_PARAM = "voldemortHomeDirectory";
    public static final String VOLDEMORT_NODE_ID_PARAM = "voldemortNodeId";
    public static final String REMOTE_TEST_ARGUMENTS_PARAM = "remoteTestArguments";
    public static final String SOURCE_DIRECTORY_PARAM = "sourceDirectory";

    private final String rawCommand;

    public CommandLineParameterizer(String commandId) {
        Properties properties = new Properties();

        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("commands.properties"));
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }

        rawCommand = properties.getProperty(commandId);
    }

    public String getRawCommand() {
        return rawCommand;
    }

    public String parameterize(Map<String, String> parameters) {
        String command = rawCommand;

        for(Map.Entry<String, String> parameter: parameters.entrySet())
            command = StringUtils.replace(command,
                                          "${" + parameter.getKey() + "}",
                                          parameter.getValue());

        return command;
    }

}

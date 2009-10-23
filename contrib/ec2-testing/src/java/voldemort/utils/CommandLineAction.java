package voldemort.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

class CommandLineAction {

    protected List<UnixCommand> generateCommands(String command,
                                                 Collection<String> hostNames,
                                                 String hostUserId,
                                                 File sshPrivateKey,
                                                 String voldemortRootDirectory,
                                                 String voldemortHomeDirectory,
                                                 File sourceDirectory) throws IOException {
        Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("commands.properties"));
        final String rawCommand = properties.getProperty(command);

        List<UnixCommand> unixCommands = new ArrayList<UnixCommand>();

        for(String hostName: hostNames) {
            String parameterizedCommand = parameterizeCommand(hostName,
                                                              hostUserId,
                                                              sshPrivateKey,
                                                              voldemortRootDirectory,
                                                              voldemortHomeDirectory,
                                                              sourceDirectory,
                                                              rawCommand);
            UnixCommand unixCommand = generateCommand(parameterizedCommand);
            unixCommands.add(unixCommand);
        }

        return unixCommands;
    }

    private String parameterizeCommand(String hostName,
                                       String hostUserId,
                                       File sshPrivateKey,
                                       String voldemortRootDirectory,
                                       String voldemortHomeDirectory,
                                       File sourceDirectory,
                                       String command) {
        Map<String, String> variableMap = new HashMap<String, String>();
        variableMap.put("hostName", hostName);
        variableMap.put("hostUserId", hostUserId);

        if(sshPrivateKey != null)
            variableMap.put("sshPrivateKey", sshPrivateKey.getAbsolutePath());

        variableMap.put("voldemortRootDirectory", voldemortRootDirectory);
        variableMap.put("voldemortHomeDirectory", voldemortHomeDirectory);

        if(sourceDirectory != null)
            variableMap.put("sourceDirectory", sourceDirectory.getAbsolutePath());

        for(Map.Entry<String, String> entry: variableMap.entrySet())
            command = StringUtils.replace(command, "${" + entry.getKey() + "}", entry.getValue());

        return command;
    }

    private UnixCommand generateCommand(String command) {
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

        System.out.println(commands);
        return new UnixCommand(commands);
    }

}

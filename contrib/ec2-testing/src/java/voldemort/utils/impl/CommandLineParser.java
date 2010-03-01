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

/**
 * CommandLineParser parses the command line into appropriate arguments. This is
 * <i>usually</i> as easy as splitting on the space character, but we also have
 * to honor quoted arguments (that may have spaces) and parse those as one
 * argument.
 * 
 * <p/>
 * 
 * This class is used in preparation for passing the list of strings to
 * UnixCommand. UnixCommand, in turn, uses the {@link java.lang.ProcessBuilder}
 * class to build up the command to execute.
 * 
 */

public class CommandLineParser {

    public List<String> parse(String command) {
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

        return commands;
    }

}

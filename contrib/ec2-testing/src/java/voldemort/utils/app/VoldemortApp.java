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

package voldemort.utils.app;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.utils.CmdUtils;

public abstract class VoldemortApp {

    protected final OptionParser parser = new OptionParser();

    protected abstract String getScriptName();

    protected abstract void run(String[] args) throws Exception;

    protected void printUsage() {
        System.err.println("Usage: $VOLDEMORT_HOME/contrib/ec2-testing/bin/" + getScriptName());

        try {
            parser.printHelpOn(System.err);
        } catch(IOException e) {
            e.printStackTrace();
        }

        System.exit(1);
    }

    protected OptionSet parse(String[] args) {
        try {
            OptionSet options = parser.parse(args);

            if(options.has("help"))
                printUsage();

            setLogging(options);
            return options;
        } catch(OptionException e) {
            System.err.println(e.getMessage());
            printUsage();
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    protected void setLogging(OptionSet options) {
        // "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
        String levelString = CmdUtils.valueOf(options, "logging", "info");

        Level level = null;

        if(levelString.equals("debug"))
            level = Level.DEBUG;
        else if(levelString.equals("info"))
            level = Level.INFO;
        else if(levelString.equals("warn"))
            level = Level.WARN;
        else if(levelString.equals("error"))
            level = Level.ERROR;
        else if(levelString.equals("off"))
            level = Level.OFF;
        else
            printUsage();

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(level);

        Enumeration<Logger> e = rootLogger.getLoggerRepository().getCurrentLoggers();

        while(e.hasMoreElements()) {
            Logger logger = e.nextElement();
            logger.setLevel(level);
        }
    }

    protected String getRequiredString(OptionSet options, String argumentName) {
        if(!options.has(argumentName)) {
            System.err.println("Missing required argument " + argumentName);
            printUsage();
        }

        return CmdUtils.valueOf(options, argumentName, "");
    }

    protected int getRequiredInt(OptionSet options, String argumentName) {
        if(!options.has(argumentName)) {
            System.err.println("Missing required argument " + argumentName);
            printUsage();
        }

        return CmdUtils.valueOf(options, argumentName, 0);
    }

    protected File getRequiredInputFile(OptionSet options, String argumentName) {
        String fileName = getRequiredString(options, argumentName);
        File file = new File(fileName);

        if(!file.canRead()) {
            System.out.println("File " + fileName + " cannot be read");
            System.exit(2);
        }

        return file;
    }

    protected File getInputFile(OptionSet options, String argumentName) {
        if(!options.has(argumentName))
            return null;

        String fileName = CmdUtils.valueOf(options, argumentName, "");
        File file = new File(fileName);

        if(!file.canRead()) {
            System.out.println("File " + fileName + " cannot be read");
            System.exit(2);
        }

        return file;
    }

    protected File getRequiredOutputFile(OptionSet options, String argumentName) {
        String fileName = getRequiredString(options, argumentName);
        File file = new File(fileName);
        File parentDirectory = file.getAbsoluteFile().getParentFile();

        if(parentDirectory == null) {
            System.out.println("File " + fileName + " does not have parent directory");
            System.exit(3);
        }

        // Try to make any parent directories for the user. Don't bother
        // checking here as we'll determine writability right below.
        parentDirectory.mkdirs();

        if(!parentDirectory.canWrite()) {
            System.out.println("File cannot be written in directory "
                               + parentDirectory.getAbsolutePath());
            System.exit(3);
        }

        return file;
    }

    protected List<String> getHostNamesFromFile(File file, boolean usePublicDnsName) {
        if(!file.canRead()) {
            System.out.println("File " + file.getAbsolutePath() + " cannot be read");
            System.exit(2);
        }

        LineIterator iterator = null;

        try {
            iterator = FileUtils.lineIterator(file);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

        List<String> hostNames = new ArrayList<String>();

        while(iterator.hasNext()) {
            String rawLine = iterator.nextLine();
            String hostName = null;

            if(usePublicDnsName) {
                int i = rawLine.indexOf(',');

                if(i != -1)
                    hostName = rawLine.substring(0, i);
                else
                    hostName = rawLine;
            } else {
                hostName = rawLine.substring(rawLine.indexOf(",") + 1, rawLine.length());
            }

            hostNames.add(hostName);
        }

        return hostNames;
    }

    protected Map<String, String> getHostNamesFromFile(File file) {
        if(!file.canRead()) {
            System.out.println("File " + file.getAbsolutePath() + " cannot be read");
            System.exit(2);
        }

        LineIterator iterator = null;

        try {
            iterator = FileUtils.lineIterator(file);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> hostNames = new HashMap<String, String>();

        while(iterator.hasNext()) {
            String rawLine = iterator.nextLine();
            String[] hostNameArray = rawLine.split(",");
            String externalHostName = hostNameArray[0];
            String internalHostName = hostNameArray.length > 1 ? hostNameArray[1]
                                                              : externalHostName;
            hostNames.put(externalHostName, internalHostName);
        }

        return hostNames;
    }

}

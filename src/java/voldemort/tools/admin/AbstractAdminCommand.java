/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.tools.admin;

import java.io.PrintStream;

import joptsimple.OptionParser;
import voldemort.VoldemortException;

/**
 * Abstract class that defines for all group commands and sub-commands
 */
public abstract class AbstractAdminCommand {

    /**
     * Initializes parser
     * 
     * @throws Exception
     */
    protected static OptionParser getParser() throws Exception {
        throw new VoldemortException("Parser initializer not implemented.");
    }

    /**
     * Prints help menu for command. If not overwritten by inherited classes, it
     * throws exception by default.
     * 
     * @param args Array of arguments for this command
     * @throws Exception
     */
    @SuppressWarnings("unused")
    public static void printHelp(PrintStream stream) throws Exception {
        throw new VoldemortException("Help menu not implemented.");
    }

    /**
     * Parses command-line and decides what help menu to be printed out. If not
     * overwritten by inherited classes, it throws exception by default.
     * 
     * @param args Array of arguments for this command
     * @throws Exception
     */
    @SuppressWarnings("unused")
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        throw new VoldemortException("Help menu not implemented.");
    }

    /**
     * Parses command-line and executes command with arguments. If not
     * overwritten by inherited classes, it throws exception by default.
     * 
     * @param args Array of arguments for this command
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        throw new VoldemortException("Command not implemented.");
    }
}

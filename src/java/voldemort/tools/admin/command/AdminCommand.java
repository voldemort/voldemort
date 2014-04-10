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

package voldemort.tools.admin.command;

import java.io.PrintStream;

import voldemort.tools.admin.AdminUtils;

/**
 * Implements all admin commands.
 */
public class AdminCommand extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to command groups or non-grouped
     * sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("async-job")) {
            AdminCommandAsyncJob.executeCommand(args);
        } else if(subCmd.equals("cleanup")) {
            AdminCommandCleanup.executeCommand(args);
        } else if(subCmd.equals("debug")) {
            AdminCommandDebug.executeCommand(args);
        } else if(subCmd.equals("meta")) {
            AdminCommandMeta.executeCommand(args);
        } else if(subCmd.equals("quota")) {
            AdminCommandQuota.executeCommand(args);
        } else if(subCmd.equals("store")) {
            AdminCommandStore.executeCommand(args);
        } else if(subCmd.equals("stream")) {
            AdminCommandStream.executeCommand(args);
        } else if(subCmd.equals("help") || subCmd.equals("--help") || subCmd.equals("-h")) {
            executeHelp(args, System.out);
        } else {
            args = AdminUtils.copyArrayAddFirst(args, subCmd);
            AdminCommandOther.executeCommand(args);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Commands");
        stream.println("-----------------------------");
        stream.println("async-job              Show or stop async jobs.");
        stream.println("cleanup                Cleanup orphaned data, vector clocks or slops.");
        stream.println("debug                  Check keys or routing plan.");
        stream.println("meta                   Get, set, clear metadata; check or sync metadata version.");
        stream.println("quota                  Get, set, unset quota or reserve memory on stores.");
        stream.println("store                  Add, delete, rollback, truncate stores, or truncate");
        stream.println("                       partitions.");
        stream.println("stream                 Fetch keys or entries, or mirror data across nodes, or");
        stream.println("                       update entries.");
        stream.println("help                   Show help menu or information for each command.");
        stream.println("native-backup          Backup bdb data on one single node natively.");
        stream.println("restore-from-replica   Restore data from peer replication.");
        stream.println();
        stream.println("To get more information on each command, please try \'help <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("async-job")) {
            AdminCommandAsyncJob.executeHelp(args, stream);
        } else if(subCmd.equals("cleanup")) {
            AdminCommandCleanup.executeHelp(args, stream);
        } else if(subCmd.equals("debug")) {
            AdminCommandDebug.executeHelp(args, stream);
        } else if(subCmd.equals("meta")) {
            AdminCommandMeta.executeHelp(args, stream);
        } else if(subCmd.equals("quota")) {
            AdminCommandQuota.executeHelp(args, stream);
        } else if(subCmd.equals("store")) {
            AdminCommandStore.executeHelp(args, stream);
        } else if(subCmd.equals("stream")) {
            AdminCommandStream.executeHelp(args, stream);
        } else {
            args = AdminUtils.copyArrayAddFirst(args, subCmd);
            AdminCommandOther.executeHelp(args, stream);
        }
    }
}

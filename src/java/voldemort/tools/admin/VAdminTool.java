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

/**
 * Provides a command line interface to the
 * {@link voldemort.client.protocol.admin.AdminClient}
 * 
 */
public class VAdminTool {
    
    /**
     * 
     * Main entry of voldemort admin commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     */
    public static void executeCommand(String[] args, Boolean printHelp) throws Exception {
        
        String grpCmd = args.length > 0 ? args[0] : "";
        if (grpCmd.equals("async-job")) AdminCommandAsyncJob.execute(args, printHelp);
        else if (grpCmd.equals("cleanup")) AdminCommandCleanup.execute(args, printHelp);
        else if (grpCmd.equals("debug")) AdminCommandDebug.execute(args, printHelp);
        else if (grpCmd.equals("meta")) AdminCommandMeta.execute(args, printHelp);
        else if (grpCmd.equals("quota")) AdminCommandQuota.execute(args, printHelp);
        else if (grpCmd.equals("store")) AdminCommandStore.execute(args, printHelp);
        else if (grpCmd.equals("stream")) AdminCommandStream.execute(args, printHelp);
        else AdminCommandOther.execute(args, printHelp);
            
    }

    public static void main(String[] args) {
        
        if (args.length < 1) {
            args = new String[1];
            args[0] = "help";
        }
        
        try {
            if (args[0].equals("help") || args[0].equals("--help") || args[0].equals("-h")) {
                String[] argCopy = new String[args.length - 1];
                System.arraycopy(args, 1, argCopy, 0, args.length - 1);
                executeCommand(argCopy, true);
            } else {
                executeCommand(args, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

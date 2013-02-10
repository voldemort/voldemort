/*
 * Copyright 2013 LinkedIn, Inc
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

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ConsistencyFixCLI {

    public static void printUsage() {
        System.out.println("Required arguments: \n" + "\t--url <url>\n" + "\t--store <storeName>\n"
                           + "\t--bad-key-file-in <FileNameOfInputListOfKeysToFix>\n"
                           + "\t--bad-key-file-out<FileNameOfOutputListOfKeysNotFixed>)\n");
    }

    public static void printUsage(String errMessage) {
        System.err.println("Error: " + errMessage);
        printUsage();
        System.exit(1);
    }

    private static class Options {

        public final static int defaultParallelism = 8;
        public final static long defaultProgressBar = 1000;
        public final static long defaultPerServerIOPSLimit = 100;

        public String url = null;
        public String storeName = null;
        public String badKeyFileIn = null;
        public boolean badKeyFileInOrphanFormat = false;
        public String badKeyFileOut = null;
        public int parallelism = defaultParallelism;
        public long progressBar = defaultProgressBar;
        public long perServerIOPSLimit = defaultPerServerIOPSLimit;
    }

    /**
     * All the logic for parsing and validating options.
     * 
     * @param args
     * @return A struct containing validated options.
     * @throws IOException
     */
    private static ConsistencyFixCLI.Options parseArgs(String[] args) {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url")
              .withRequiredArg()
              .describedAs("The bootstrap url.")
              .ofType(String.class);
        parser.accepts("store")
              .withRequiredArg()
              .describedAs("The store name.")
              .ofType(String.class);
        parser.accepts("bad-key-file-in")
              .withRequiredArg()
              .describedAs("Name of bad-key-file-in. " + "Each key must be in hexadecimal format. "
                           + "Each key must be on a separate line in the file. ")
              .ofType(String.class);
        parser.accepts("orphan-format",
                       "Indicates format of bad-key-file-in is of 'orphan' key-values.");
        parser.accepts("bad-key-file-out")
              .withRequiredArg()
              .describedAs("Name of bad-key-file-out. "
                           + "Keys that are not mae consistent are output to this file.")
              .ofType(String.class);
        parser.accepts("parallelism")
              .withRequiredArg()
              .describedAs("Number of consistency fix messages outstanding in parallel. "
                           + "[Default value: " + Options.defaultParallelism + "]")
              .ofType(Integer.class);
        parser.accepts("progress-bar")
              .withRequiredArg()
              .describedAs("Number of operations between 'info' progress messages. "
                           + "[Default value: " + Options.defaultProgressBar + "]")
              .ofType(Long.class);
        parser.accepts("per-server-iops-limit")
              .withRequiredArg()
              .describedAs("Number of operations that the consistency fixer will issue into any individual server in one second. "
                           + "[Default value: " + Options.defaultPerServerIOPSLimit + "]")
              .ofType(Long.class);

        OptionSet optionSet = parser.parse(args);

        if(optionSet.hasArgument("help")) {
            try {
                parser.printHelpOn(System.out);
            } catch(IOException e) {
                e.printStackTrace();
            }
            printUsage();
            System.exit(0);
        }
        if(!optionSet.hasArgument("url")) {
            printUsage("Missing required 'url' argument.");
        }
        if(!optionSet.hasArgument("store")) {
            printUsage("Missing required 'store' argument.");
        }
        if(!optionSet.has("bad-key-file-in")) {
            printUsage("Missing required 'bad-key-file-in' argument.");
        }
        if(!optionSet.has("bad-key-file-out")) {
            printUsage("Missing required 'bad-key-file-out' argument.");
        }

        Options options = new Options();

        options.url = (String) optionSet.valueOf("url");
        options.storeName = (String) optionSet.valueOf("store");
        options.badKeyFileIn = (String) optionSet.valueOf("bad-key-file-in");
        options.badKeyFileOut = (String) optionSet.valueOf("bad-key-file-out");
        if(optionSet.has("orphan-format")) {
            options.badKeyFileInOrphanFormat = true;
        }
        if(optionSet.has("parallelism")) {
            options.parallelism = (Integer) optionSet.valueOf("parallelism");
        }
        if(optionSet.has("progress-bar")) {
            options.progressBar = (Long) optionSet.valueOf("progress-bar");
        }
        if(optionSet.has("per-server-iops-limit")) {
            options.perServerIOPSLimit = (Long) optionSet.valueOf("per-server-iops-limit");
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        Options options = parseArgs(args);

        ConsistencyFix consistencyFix = new ConsistencyFix(options.url,
                                                           options.storeName,
                                                           options.progressBar,
                                                           options.perServerIOPSLimit);

        String summary = consistencyFix.execute(options.parallelism,
                                                options.badKeyFileIn,
                                                options.badKeyFileInOrphanFormat,
                                                options.badKeyFileOut);

        System.out.println(summary);
    }
}

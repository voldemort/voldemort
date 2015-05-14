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
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("Required arguments: \n");
        sb.append("\t--url <url>\n");
        sb.append("\t--store <storeName>\n");
        sb.append("\t--bad-key-file-in  <FileNameOfInputListOfKeysToFix>\n");
        sb.append("\t--bad-key-file-out <FileNameOfOutputListOfKeysNotFixed>)\n");
        sb.append("Optional arguments: \n");
        sb.append("\t--orphan-format\n");
        sb.append("\t--dry-run\n");
        sb.append("\t--progress-period-ops <progressPeriodOps>\n");
        sb.append("\t--parallelism <parallelism>\n");
        sb.append("\t--per-server-iops-limit <perServerIOPSLimit>\n");
        sb.append("\n");

        System.out.println(sb.toString());
    }

    public static void printUsage(String errMessage, OptionParser parser) {
        System.err.println("Error: " + errMessage);
        try {
            parser.printHelpOn(System.out);
        } catch(IOException ioe) {
            System.err.println("Caught IOException while trying to print out parser options: "
                               + ioe.getMessage());
        }
        printUsage();
        System.exit(1);
    }

    private static class Options {

        public final static int defaultParallelism = 8;
        public final static long defaultProgressPeriodOps = 1000;
        public final static long defaultPerServerQPSLimit = 100;

        public String url = null;
        public String storeName = null;
        public String badKeyFileIn = null;
        public boolean badKeyFileInOrphanFormat = false;
        public String badKeyFileOut = null;
        public int parallelism = defaultParallelism;
        public long progressPeriodOps = defaultProgressPeriodOps;
        public long perServerQPSLimit = defaultPerServerQPSLimit;
        public boolean dryRun = false;
        public boolean parseOnly = false;
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
        parser.accepts("url", "The bootstrap url.")
              .withRequiredArg()
              .describedAs("bootstrapUrl")
              .ofType(String.class);
        parser.accepts("store", "The store name.")
              .withRequiredArg()
              .describedAs("storeName")
              .ofType(String.class);
        parser.accepts("bad-key-file-in",
                       "Name of bad-key-file-in. " + "Each key must be in hexadecimal format. "
                               + "Each key must be on a separate line in the file. ")
              .withRequiredArg()
              .describedAs("badKeyFileIn")
              .ofType(String.class);
        parser.accepts("orphan-format",
                       "Indicates format of bad-key-file-in is of 'orphan' key-values.");
        parser.accepts("dry-run",
                       "Indicates to go through all of the read actions until the point of issuing repair puts. Then, do a 'no-op'.");
        parser.accepts("parse-only",
                       "Indicates to only parse the input file. Does not perform any key queries or repair puts. "
                               + "Does bootstrap though so bootstrapUrl and storeName must be specified.");
        parser.accepts("bad-key-file-out",
                       "Name of bad-key-file-out. "
                               + "Keys that are not mae consistent are output to this file.")
              .withRequiredArg()
              .describedAs("badKeyFileOut")
              .ofType(String.class);
        parser.accepts("parallelism",
                       "Number of consistency fix messages outstanding in parallel. ")
              .withRequiredArg()
              .describedAs("parallelism [Default value: " + Options.defaultParallelism + "]")
              .ofType(Integer.class);
        parser.accepts("progress-period-ops",
                       "Number of operations between 'info' progress messages. ")
              .withRequiredArg()
              .describedAs("period (in operations) between outputting progress [Default value: "
                           + Options.defaultProgressPeriodOps + "]")
              .ofType(Long.class);
        parser.accepts("per-server-qps-limit",
                       "Number of operations that the consistency fixer will issue to any individual server in one second. ")
              .withRequiredArg()
              .describedAs("perServerQPSLimit [Default value: " + Options.defaultPerServerQPSLimit
                           + "]")
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
            printUsage("Missing required 'url' argument.", parser);
        }
        if(!optionSet.hasArgument("store")) {
            printUsage("Missing required 'store' argument.", parser);
        }
        if(!optionSet.has("bad-key-file-in")) {
            printUsage("Missing required 'bad-key-file-in' argument.", parser);
        }
        if(!optionSet.has("bad-key-file-out")) {
            printUsage("Missing required 'bad-key-file-out' argument.", parser);
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
        if(optionSet.has("progress-period-ops")) {
            options.progressPeriodOps = (Long) optionSet.valueOf("progress-period-ops");
        }
        if(optionSet.has("per-server-qps-limit")) {
            options.perServerQPSLimit = (Long) optionSet.valueOf("per-server-qps-limit");
        }
        if(optionSet.has("dry-run")) {
            options.dryRun = true;
        }
        if(optionSet.has("parse-only")) {
            options.parseOnly = true;
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        Options options = parseArgs(args);

        ConsistencyFix consistencyFix = new ConsistencyFix(options.url,
                                                           options.storeName,
                                                           options.progressPeriodOps,
                                                           options.perServerQPSLimit,
                                                           options.dryRun,
                                                           options.parseOnly);

        String summary = consistencyFix.execute(options.parallelism,
                                                options.badKeyFileIn,
                                                options.badKeyFileInOrphanFormat,
                                                options.badKeyFileOut);

        consistencyFix.close();

        System.out.println(summary);
    }
}

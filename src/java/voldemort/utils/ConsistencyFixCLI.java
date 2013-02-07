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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.utils.ConsistencyFix.BadKeyInput;
import voldemort.utils.ConsistencyFix.BadKeyResult;

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

        public final static int defaultParallelism = 2;

        public String url = null;
        public String storeName = null;
        public String badKeyFileIn = null;
        public String badKeyFileOut = null;
        public int parallelism = 0;
        public boolean verbose = false;
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
        parser.accepts("bad-key-file-out")
              .withRequiredArg()
              .describedAs("Name of bad-key-file-out. "
                           + "Keys that are not mae consistent are output to this file.")
              .ofType(String.class);
        parser.accepts("parallelism")
              .withOptionalArg()
              .describedAs("Number of read and to repair in parallel. "
                           + "Up to 2X this value requests outstanding simultaneously. "
                           + "[Default value: " + Options.defaultParallelism + "]")
              .ofType(Integer.class);

        parser.accepts("verbose", "verbose");
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
        options.badKeyFileOut = (String) optionSet.valueOf("bad-key-file-out");
        options.badKeyFileIn = (String) optionSet.valueOf("bad-key-file-in");
        options.parallelism = Options.defaultParallelism;
        if(optionSet.has("parallelism")) {
            options.parallelism = (Integer) optionSet.valueOf("parallelism");
        }

        if(optionSet.has("verbose")) {
            options.verbose = true;
        }

        return options;
    }

    private static ExecutorService badKeyReaderService;
    private static ExecutorService badKeyWriterService;
    private static ExecutorService badKeyGetters;
    private static ExecutorService repairPutters;

    // TODO: Should all of this executor service stuff be in this class? Or, in
    // ConsistencyFix?

    // TODO: Did I do anything stupid to parallelize this work? I have much more
    // machinery than I expected (four executor services!) and I am not
    // particularly fond of the "poison" types used to tear down the threads
    // that depend on BlockingQueues (BadKeyREader and BadKeyWriter).
    public static void main(String[] args) throws Exception {
        Options options = parseArgs(args);

        ConsistencyFix consistencyFix = new ConsistencyFix(options.url, options.storeName);
        System.out.println("Constructed the consistency fixer..");

        BlockingQueue<BadKeyInput> badKeyQIn = new ArrayBlockingQueue<BadKeyInput>(1000);
        badKeyReaderService = Executors.newSingleThreadExecutor();
        badKeyReaderService.submit(consistencyFix.new BadKeyReader(options.badKeyFileIn, badKeyQIn));
        System.out.println("Created badKeyReader.");

        BlockingQueue<BadKeyResult> badKeyQOut = new ArrayBlockingQueue<BadKeyResult>(1000);
        badKeyWriterService = Executors.newSingleThreadExecutor();
        badKeyWriterService.submit(consistencyFix.new BadKeyWriter(options.badKeyFileOut,
                                                                   badKeyQOut));
        System.out.println("Created badKeyWriter.");

        CountDownLatch latch = new CountDownLatch(options.parallelism);
        badKeyGetters = Executors.newFixedThreadPool(options.parallelism);
        repairPutters = Executors.newFixedThreadPool(options.parallelism);
        System.out.println("Created getters & putters.");

        for(int i = 0; i < options.parallelism; i++) {
            badKeyGetters.submit(new ConsistencyFixKeyGetter(latch,
                                                             consistencyFix,
                                                             repairPutters,
                                                             badKeyQIn,
                                                             badKeyQOut,
                                                             options.verbose));
        }

        latch.await();
        System.out.println("All badKeyGetters have completed.");

        badKeyReaderService.shutdown();
        badKeyReaderService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("Bad key reader service has shutdown.");

        badKeyGetters.shutdown();
        badKeyGetters.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("All badKeyGetters have shutdown.");

        repairPutters.shutdown();
        repairPutters.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("All repairPutters have shutdown.");

        // Poison the bad key writer.
        badKeyQOut.put(consistencyFix.new BadKeyResult());
        badKeyWriterService.shutdown();
        badKeyWriterService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("Bad key writer service has shutdown.");

        consistencyFix.stop();
        System.out.println("Stopped the consistency fixer..");

    }

}

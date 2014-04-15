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

import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

/**
 * Parser utility class for AdminCommand
 * 
 */
public class AdminParserUtils {

    public static void printArgs(String[] args) {
        System.out.println("Arguments Accepted");
        for(String arg: args) {
            System.out.println(arg);
        }
    }

    // options without argument
    public static final String OPT_ALL_NODES = "all-nodes";
    public static final String OPT_ALL_PARTITIONS = "all-partitions";
    public static final String OPT_ALL_STORES = "all-stores";
    public static final String OPT_CONFIRM = "confirm";
    public static final String OPT_H = "h";
    public static final String OPT_HELP = "help";
    public static final String OPT_ORPHANED = "orphaned";

    // options with one argument
    public static final String OPT_D = "d";
    public static final String OPT_DIR = "dir";
    public static final String OPT_F = "f";
    public static final String OPT_FILE = "file";
    public static final String OPT_FORMAT = "format";
    public static final String OPT_U = "u";
    public static final String OPT_URL = "url";
    public static final String OPT_Z = "z";
    public static final String OPT_ZONE = "zone";

    // options with multiple arguments
    public static final String OPT_X = "x";
    public static final String OPT_HEX = "hex";
    public static final String OPT_J = "j";
    public static final String OPT_JSON = "json";
    public static final String OPT_P = "p";
    public static final String OPT_PARTITION = "partition";

    // options that have either one argument or multiple arguments
    public static final String OPT_N = "n";
    public static final String OPT_NODE = "node";
    public static final String OPT_S = "s";
    public static final String OPT_STORE = "store";

    // defined argument strings
    public static final String ARG_FORMAT_BINARY = "binary";
    public static final String ARG_FORMAT_HEX = "hex";
    public static final String ARG_FORMAT_JSON = "json";

    /**
     * Adds OPT_ALL_NODES option to OptionParser, without argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsAllNodes(OptionParser parser) {
        parser.accepts(OPT_ALL_NODES, "select all nodes");
    }

    /**
     * Adds OPT_ALL_PARTITIONS option to OptionParser, without argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsAllPartitions(OptionParser parser) {
        parser.accepts(OPT_ALL_PARTITIONS, "select all partitions");
    }

    /**
     * Adds OPT_ALL_STORES option to OptionParser, without argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsAllStores(OptionParser parser) {
        parser.accepts(OPT_ALL_STORES, "select all stores");
    }

    /**
     * Adds OPT_CONFIRM option to OptionParser, without argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsConfirm(OptionParser parser) {
        parser.accepts(OPT_CONFIRM, "confirm dangerous operation");
    }

    /**
     * Adds OPT_H | OPT_HELP option to OptionParser, without argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsHelp(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_H, OPT_HELP), "show help menu");
    }

    /**
     * Adds OPT_ORPHANED option to OptionParser, without argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsOrphaned(OptionParser parser) {
        parser.accepts(OPT_ORPHANED, "fetch orphaned keys or entries");
    }

    /**
     * Adds OPT_D | OPT_DIR option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsDir(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_D, OPT_DIR), "directory path for input/output")
              .withRequiredArg()
              .describedAs("dir-path")
              .ofType(String.class);
    }

    /**
     * Adds OPT_F | OPT_FILE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsFile(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_F, OPT_FILE), "file path for input/output")
              .withRequiredArg()
              .describedAs("file-path")
              .ofType(String.class);
    }

    /**
     * Adds OPT_FORMAT option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsFormat(OptionParser parser) {
        parser.accepts(OPT_FORMAT, "format of key or entry, could be binary, hex or json")
              .withRequiredArg()
              .describedAs("binary | hex | json")
              .ofType(String.class);
    }

    /**
     * Adds OPT_N | OPT_NODE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsNodeSingle(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_N, OPT_NODE), "node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
    }

    /**
     * Adds OPT_S | OPT_STORE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsStoreSingle(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_S, OPT_STORE), "store name")
              .withRequiredArg()
              .describedAs("store-name")
              .ofType(String.class);
    }

    /**
     * Adds OPT_U | OPT_URL option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsUrl(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_U, OPT_URL), "bootstrap url")
              .withRequiredArg()
              .describedAs("url")
              .ofType(String.class);
    }

    /**
     * Adds OPT_Z | OPT_ZONE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsZone(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_Z, OPT_ZONE), "zone id")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(Integer.class);
    }

    /**
     * Adds OPT_X | OPT_HEX option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsHex(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_X, OPT_HEX), "fetch key/entry by key value of hex type")
              .withRequiredArg()
              .describedAs("key-list")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
    }

    /**
     * Adds OPT_J | OPT_JSON option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsJson(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_J, OPT_JSON),
                          "fetch key/entry by key value of json type")
              .withRequiredArg()
              .describedAs("key-list")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
    }

    /**
     * Adds OPT_N | OPT_NODE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsNodeMultiple(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_N, OPT_NODE), "node id")
              .withRequiredArg()
              .describedAs("node-id")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
    }

    /**
     * Adds OPT_P | OPT_PARTITION option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsPartition(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_P, OPT_PARTITION), "partition id list")
              .withRequiredArg()
              .describedAs("partition-id-list")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
    }

    /**
     * Adds OPT_S | OPT_STORE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsStoreMultiple(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_S, OPT_STORE), "store name")
              .withRequiredArg()
              .describedAs("store-name")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
    }

    /**
     * Checks if the required option exists.
     * 
     * @param options OptionSet to checked
     * @param opt Required option to check
     * @throws VoldemortException
     */
    public static void checkRequired(OptionSet options, String opt) throws VoldemortException {
        List<String> opts = Lists.newArrayList();
        opts.add(opt);
        checkRequired(options, opts);
    }

    /**
     * Checks if there's exactly one option exists among all possible opts.
     * 
     * @param options OptionSet to checked
     * @param opt1 Possible required option to check
     * @param opt2 Possible required option to check
     * @throws VoldemortException
     */
    public static void checkRequired(OptionSet options, String opt1, String opt2)
            throws VoldemortException {
        List<String> opts = Lists.newArrayList();
        opts.add(opt1);
        opts.add(opt2);
        checkRequired(options, opts);
    }

    /**
     * Checks if there's exactly one option exists among all possible opts.
     * 
     * @param options OptionSet to checked
     * @param opt1 Possible required option to check
     * @param opt2 Possible required option to check
     * @param opt3 Possible required option to check
     * @throws VoldemortException
     */
    public static void checkRequired(OptionSet options, String opt1, String opt2, String opt3)
            throws VoldemortException {
        List<String> opts = Lists.newArrayList();
        opts.add(opt1);
        opts.add(opt2);
        opts.add(opt3);
        checkRequired(options, opts);
    }

    /**
     * Checks if there's exactly one option exists among all opts.
     * 
     * @param options OptionSet to checked
     * @param opts List of options to be checked
     * @throws VoldemortException
     */
    public static void checkRequired(OptionSet options, List<String> opts)
            throws VoldemortException {
        List<String> optCopy = Lists.newArrayList();
        for(String opt: opts) {
            if(options.has(opt)) {
                optCopy.add(opt);
            }
        }
        if(optCopy.size() < 1) {
            System.err.println("Please specify one of the following options:");
            for(String opt: opts) {
                System.err.println("--" + opt);
            }
            Utils.croak("Missing required option.");
        }
        if(optCopy.size() > 1) {
            System.err.println("Conflicting options:");
            for(String opt: optCopy) {
                System.err.println("--" + opt);
            }
            Utils.croak("Conflicting options detected.");
        }
    }

    /**
     * Checks if there's at most one option exists among all opts.
     * 
     * @param parser OptionParser to checked
     * @param opt1 Optional option to check
     * @param opt2 Optional option to check
     * @throws VoldemortException
     */
    public static void checkOptional(OptionSet options, String opt1, String opt2) {
        List<String> opts = Lists.newArrayList();
        opts.add(opt1);
        opts.add(opt2);
        checkOptional(options, opts);
    }

    /**
     * Checks if there's at most one option exists among all opts.
     * 
     * @param parser OptionParser to checked
     * @param opt1 Optional option to check
     * @param opt2 Optional option to check
     * @param opt3 Optional option to check
     * @throws VoldemortException
     */
    public static void checkOptional(OptionSet options, String opt1, String opt2, String opt3) {
        List<String> opts = Lists.newArrayList();
        opts.add(opt1);
        opts.add(opt2);
        opts.add(opt3);
        checkOptional(options, opts);
    }

    /**
     * Checks if there's at most one option exists among all opts.
     * 
     * @param parser OptionParser to checked
     * @param opts List of options to be checked
     * @throws VoldemortException
     */
    public static void checkOptional(OptionSet options, List<String> opts)
            throws VoldemortException {
        List<String> optCopy = Lists.newArrayList();
        for(String opt: opts) {
            if(options.has(opt)) {
                optCopy.add(opt);
            }
        }
        if(optCopy.size() > 1) {
            System.err.println("Conflicting options:");
            for(String opt: optCopy) {
                System.err.println("--" + opt);
            }
            throw new VoldemortException("Conflicting options detected.");
        }
    }
}

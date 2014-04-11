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


import java.io.IOException;
import java.util.*;

import com.google.common.collect.Lists;

import voldemort.VoldemortException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Parser which defines admin options, checks and parses command-line input.
 * 
 */
public class AdminOptionParser {

    // head arguments
    public static final String OPT_HEAD_ASYNC_JOB_STOP = "async-job-stop";
    public static final String OPT_HEAD_QUOTA_RESERVE_MEMORY = "quota-reserve-memory";
    public static final String OPT_HEAD_META_CHECK = "meta-check";
    public static final String OPT_HEAD_META_GET = "meta-get";
    public static final String OPT_HEAD_META_SET = "meta-set";
    public static final String OPT_HEAD_QUOTA_GET = "quota-get";
    public static final String OPT_HEAD_QUOTA_SET = "quota-set";
    public static final String OPT_HEAD_QUOTA_UNSET = "quota-unset";
    
    // options without argument
    public static final String OPT_ALL_NODES = "all-nodes";
    public static final String OPT_ALL_PARTITIONS = "all-partitions";
    public static final String OPT_ALL_STORES = "all-stores";
    public static final String OPT_CONFIRM = "confirm";
    public static final String OPT_INCREMENTAL = "incremental";
    public static final String OPT_ORPHANED = "orphaned";
    public static final String OPT_VERBOSE = "verbose";
    public static final String OPT_VERIFY = "verify";

    // options with one argument
    public static final String OPT_DIR = "dir";
    public static final String OPT_FILE = "file";
    public static final String OPT_FORMAT = "format";
    public static final String OPT_FROM_NODE = "from-node";
    public static final String OPT_FROM_URL = "from-url";
    public static final String OPT_PARALLEL = "parallel";
    public static final String OPT_TIMEOUT = "timeout";
    public static final String OPT_TO_NODE = "to-node";
    public static final String OPT_TO_URL = "to-url";
    public static final String OPT_URL = "url";
    public static final String OPT_VERSION = "version";
    public static final String OPT_ZONE = "zone";

    // options with multiple arguments
    public static final String OPT_HEX = "hex";
    public static final String OPT_JSON = "json";
    public static final String OPT_PARTITION = "partition";

    // options that have either one argument or multiple arguments
    public static final String OPT_NODE = "node"; // don't use it in addRequired() or addOptional()
    public static final String OPT_NODE_SINGLE = "node-single";
    public static final String OPT_NODE_MULTIPLE = "node-multiple";
    public static final String OPT_STORE = "store"; // don't use it in addRequired() or addOptional()
    public static final String OPT_STORE_SINGLE = "store-single";
    public static final String OPT_STORE_MULTIPLE = "store-multiple";

    // defined argument strings
    public static final String ARG_FORMAT_BINARY = "binary";
    public static final String ARG_FORMAT_HEX = "hex";
    public static final String ARG_FORMAT_JSON = "json";
    
    private OptionParser parser;
    private OptionSet options;
    private Set<List<String>> required; // exactly one opt in List<String> exists
    private Set<List<String>> optional; // at most one opt in List<String> exists
    private Map<String, AdminOption<?>> optMap;
    private String optHead;

    /**
     * Constructs containers and defines admin options.
     */
    public AdminOptionParser() {
        
        optHead = null;
        required = new HashSet<List<String>>();
        optional = new HashSet<List<String>>();
        optMap = new HashMap<String, AdminOption<?>>();
        parser = new OptionParser();
        
        // construct admin options
        // head arguments
        optMap.put(OPT_HEAD_ASYNC_JOB_STOP, new AdminOption<Integer>(OPT_HEAD_ASYNC_JOB_STOP,
                "list of job ids to be stopped", "job-id-list", ',', Integer.class));
        optMap.put(OPT_HEAD_QUOTA_RESERVE_MEMORY, new AdminOption<Integer>(OPT_HEAD_QUOTA_RESERVE_MEMORY,
                "memory size in MB to be reserved", "memory-size", Integer.class));
        optMap.put(OPT_HEAD_META_CHECK, new AdminOption<String>(OPT_HEAD_META_CHECK,
                "metadata keys to be checked", "meta-key-list", ',', String.class));
        optMap.put(OPT_HEAD_META_GET, new AdminOption<String>(OPT_HEAD_META_GET,
                "metadata keys to fetch", "meta-key-list", ',', String.class));
        optMap.put(OPT_HEAD_META_SET, new AdminOption<String>(OPT_HEAD_META_SET,
                "specify metadata files to set metadata keys", "meta-key>=<meta-file", ',', String.class));
        optMap.put(OPT_HEAD_QUOTA_GET, new AdminOption<String>(OPT_HEAD_QUOTA_GET,
                "quota types to fetch", "quota-type-list", ',', String.class));
        optMap.put(OPT_HEAD_QUOTA_SET, new AdminOption<String>(OPT_HEAD_QUOTA_SET,
                "specify quota values", "quota-type>=<quota-value", ',', String.class));
        optMap.put(OPT_HEAD_QUOTA_UNSET, new AdminOption<String>(OPT_HEAD_QUOTA_UNSET,
                "quota types to unset", "quota-type-list", ',', String.class));

        // options without argument
        optMap.put(OPT_ALL_NODES, new AdminOption<Boolean>(OPT_ALL_NODES,
                "select all nodes", Boolean.class));
        optMap.put(OPT_ALL_PARTITIONS, new AdminOption<Boolean>(OPT_ALL_PARTITIONS,
                "select all partitions", Boolean.class));
        optMap.put(OPT_ALL_STORES, new AdminOption<Boolean>(OPT_ALL_STORES,
                "select all stores", Boolean.class));
        
        optMap.put(OPT_CONFIRM, new AdminOption<Boolean>(OPT_CONFIRM,
                "confirm dangerous operations", Boolean.class));
        optMap.put(OPT_INCREMENTAL, new AdminOption<String>(OPT_INCREMENTAL,
                "incremental native-backup for point-in-time recovery", String.class));
        optMap.put(OPT_ORPHANED, new AdminOption<String>(OPT_ORPHANED,
                "fetch orphaned key/entry", String.class));
        optMap.put(OPT_VERBOSE, new AdminOption<Boolean>(OPT_VERBOSE,
                "print all metadata", Boolean.class));
        optMap.put(OPT_VERIFY, new AdminOption<String>(OPT_VERIFY,
                "native-backup verify checksum", String.class));

        // options with one argument
        optMap.put(OPT_DIR, new AdminOption<String>(OPT_DIR,
                "directory path for input/output", "dir-path", String.class));
        optMap.put(OPT_FILE, new AdminOption<String>(OPT_FILE,
                "file path for input/output", "file-path", String.class));
        optMap.put(OPT_FORMAT, new AdminOption<String>(OPT_FORMAT,
                "format of key or entry, could be binary, hex or json", "binary | hex | json", String.class));
        optMap.put(OPT_FROM_NODE, new AdminOption<Integer>(OPT_FROM_NODE,
                "mirror source node id", "src-node-id", Integer.class));
        optMap.put(OPT_FROM_URL, new AdminOption<String>(OPT_FROM_URL,
                "mirror source bootstrap url", "src-url", String.class));
        optMap.put(OPT_PARALLEL, new AdminOption<Integer>(OPT_PARALLEL,
                "parallism parameter for restore-from-replica", "num", Integer.class));
        optMap.put(OPT_TIMEOUT, new AdminOption<Integer>(OPT_TIMEOUT,
                "native-backup timeout in minute", "time-minute", Integer.class));
        optMap.put(OPT_TO_NODE, new AdminOption<Integer>(OPT_TO_NODE,
                "mirror destination node id", "dest-node-id", Integer.class));
        optMap.put(OPT_TO_URL, new AdminOption<String>(OPT_TO_URL,
                "mirror destination bootstrap url", "dest-url", String.class));
        optMap.put(OPT_URL, new AdminOption<String>(OPT_URL,
                "bootstrap url", "url", String.class));
        optMap.put(OPT_VERSION, new AdminOption<Long>(OPT_VERSION,
                "rollback-ro version", "store-version", Long.class));
        optMap.put(OPT_ZONE, new AdminOption<String>(OPT_ZONE,
                "zone id", "zone-id", String.class));

        // options with multiple arguments
        optMap.put(OPT_HEX, new AdminOption<String>(OPT_HEX,
                "fetch key/entry by key value of hex type", "key-list", ',', String.class));
        optMap.put(OPT_JSON, new AdminOption<String>(OPT_JSON,
                "fetch key/entry by key value of json type", "key-list", ',', String.class));
        optMap.put(OPT_PARTITION, new AdminOption<Integer>(OPT_PARTITION,
                "partition id list", "partition-id-list", ',', Integer.class));

        // options that have either one argument or multiple arguments
        optMap.put(OPT_NODE_SINGLE, new AdminOption<Integer>(OPT_NODE,
                "node id", "node-id", Integer.class));
        optMap.put(OPT_STORE_SINGLE, new AdminOption<String>(OPT_STORE,
                "store name", "store-name", String.class));
        optMap.put(OPT_NODE_MULTIPLE, new AdminOption<Integer>(OPT_NODE,
                "node id list", "node-id-list", ',', Integer.class));
        optMap.put(OPT_STORE_MULTIPLE, new AdminOption<String>(OPT_STORE,
                "store name list", "store-name-list", ',', String.class));
    }

    /**
     * Adds a required option.
     * 
     * @param opt Name of option to be accepted
     */
    public void addRequired(String opt) {
        List<String> opts = new ArrayList<String>();
        opts.add(opt);
        addRequired(opts);
    }

    /**
     * Adds an optional option.
     * 
     * @param opt Name of option to be accepted
     */
    public void addOptional(String opt) {
        List<String> opts = new ArrayList<String>();
        opts.add(opt);
        addOptional(opts);
    }

    /**
     * Adds a pair of required options, exactly one of them should exist in command-line.
     * 
     * @param opt1 Name of option to be accepted
     * @param opt2 Name of option to be accepted
     */
    public void addRequired(String opt1, String opt2) {
        List<String> opts = new ArrayList<String>();
        opts.add(opt1);
        opts.add(opt2);
        addRequired(opts);
    }

    /**
     * Adds a pair of optional options, at most one of them could exist in command-line.
     * 
     * @param opt1 Name of option to be accepted
     * @param opt2 Name of option to be accepted
     */
    public void addOptional(String opt1, String opt2) {
        List<String> opts = new ArrayList<String>();
        opts.add(opt1);
        opts.add(opt2);
        addOptional(opts);
    }

    /**
     * Adds head argument that follows command name
     * 
     * @param opt Head argument to be accepted
     * 
     */
    public void addHeadArgument(String opt) {
        optHead = opt;
        optMap.get(optHead).join(parser);
    }
    
    /**
     * Adds a list of required options, exactly one of them should exist in command-line.
     * 
     * @param opts List of options to be accepted
     * 
     */
    public void addRequired(List<String> opts) {
        if (optional.contains(opts)) optional.remove(opts);
        if (!required.contains(opts)) required.add(opts);
        for (String opt: opts) optMap.get(opt).join(parser);
    }
    
    /**
     * Adds a list of optional options, at most one of them could exist in command-line.
     * 
     * @param opts List of options to be accepted
     */
    public void addOptional(List<String> opts) {
        if (required.contains(opts)) required.remove(opts);
        if (!optional.contains(opts)) optional.add(opts);
        for (String opt: opts) optMap.get(opt).join(parser);
    }

    /**
     * Parses and Checks the command-line input.
     * 
     * @param args Command-line input
     * @param start Number of leading arguments to be skipped
     * @throws VoldemortException
     */
    public void parse(String[] args, int start) throws Exception {
        
        String[] argCopy;
        
        if (args.length <= start) {
            throw new VoldemortException("insufficient arguments");
        }
        
        if (optHead != null) {
            argCopy = new String[args.length - start + 1];
            argCopy[0] = "--" + optHead;
            System.arraycopy(args, start, argCopy, 1, argCopy.length - 1);
        } else {
            argCopy = new String[args.length - start];
            System.arraycopy(args,  start, argCopy, 0, argCopy.length);
        }

        Iterator<List<String>> iter;
        
        // jopt parser parses command-line
        options = parser.parse(argCopy);
        
        // check missing or duplicated required options
        iter = required.iterator();
        while (iter.hasNext()) {
            List<String> opts = iter.next();
            Integer count = 0;
            for (String opt: opts) if (options.has(opt)) count++;
            if (count < 1) { 
                throw new VoldemortException("missing option: --" + opts.get(0));
            }
            if (count > 1) {
                throw new VoldemortException("duplicated option: --" + opts.get(0));
            }
        }
        
        // check duplicated optional options
        iter = optional.iterator();
        while (iter.hasNext()) {
            List<String> opts = iter.next();
            Integer count = 0;
            for (String opt: opts) {
                if (options.has(opt)) count++;
            }
            if (count > 1) {
                throw new VoldemortException("duplicated option: --" + opts.get(0));
            }
        }
    }

    /**
     * Tells whether the given option was detected.
     * 
     * @param opt the option to search for
     * @returns True if option was detected
     */
    public boolean hasOption(String opt) {
        return options.has(opt);
    }

    /**
     * Gives the argument associated with the given option.
     * 
     * @param opt the option to search for
     * @returns the argument of the given option; null if no argument is present,
     *  or that option was not detected
     */
    @SuppressWarnings("unchecked")
    public <V> V getValue(String opt) {
        if (options.has(opt)) {
            return (V) options.valueOf(opt);
        } else {
            return null;
        }
    }

    /**
     * Gives any arguments associated with the given option.
     * 
     * @param opt the option to search for
     * @returns the arguments associated with the option,
     *  as a list of objects of the type given to the arguments;
     *  an empty list if no such arguments are present, or if the option was not detected
     */
    public List<?> getValueList(String opt) {
        if (options.has(opt)) {
            return (List<?>) options.valuesOf(opt);
        } else {
            return null;
        }
    }
    
    /**
     * Gives a pair of string arguments associated with the given option.
     * 
     * @param opt The option to search for
     * @param delim Delimiter that separates the argument pair (different with option delimiter)
     * @returns The list of pairs of arguments of the given option; null if no argument is present,
     *  or that option was not detected. The even elements are the first ones of the argument pair,
     *  and the odd elements are the second ones. For example, if the argument of the option is
     *  "cluster.xml=file1,stores.xml=file2", option delimiter is ',' and the pair delimiter is '=',
     *  we will then have the list of strings in return:
     *  ["cluster.xml", "file1", "stores.xml", "file2"].
     */
    @SuppressWarnings("unchecked")
    public List<String> getValuePairList(String opt, String delim) {
        List<String> valueList = (List<String>) getValueList(opt);
        List<String> valuePairList = Lists.newArrayList();
        for (String value: valueList) {
            String[] valuePair = value.split(delim, 2);
            if (valuePair.length != 2) throw new VoldemortException("Invalid argument pair: " + value);
            valuePairList.add(valuePair[0]);
            valuePairList.add(valuePair[1]);            
        }
        return valuePairList;
    }

    /**
     * Prints accepted options and descriptions.
     * @throws IOException 
     * 
     */
    public void printHelp() throws IOException {
        parser.printHelpOn(System.out);
    }

}

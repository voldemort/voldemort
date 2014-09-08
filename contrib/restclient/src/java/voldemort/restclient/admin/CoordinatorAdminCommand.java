package voldemort.restclient.admin;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.io.IOUtils;

import voldemort.VoldemortException;
import voldemort.tools.admin.command.AbstractAdminCommand;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Implements all coordinator admin commands.
 */
public class CoordinatorAdminCommand extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to command groups or non-grouped
     * sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = CoordinatorAdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("get")) {
            SubCommandGet.executeCommand(args);
        } else if(subCmd.equals("put")) {
            SubCommandPut.executeCommand(args);
        } else if(subCmd.equals("delete")) {
            SubCommandDelete.executeCommand(args);
        } else {
            args = CoordinatorAdminUtils.copyArrayAddFirst(args, subCmd);
            executeHelp(args, System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Coordinator Admin Tool");
        stream.println("--------------------------------");
        stream.println("get          Get store client config for stores.");
        stream.println("put          Put store client config for stroes from input string or given avro file");
        stream.println("delete       Delete store client config for stores");
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
        args = CoordinatorAdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("get")) {
            SubCommandGet.printHelp(stream);
        } else if(subCmd.equals("put")) {
            SubCommandPut.printHelp(stream);
        } else if(subCmd.equals("delete")) {
            SubCommandDelete.printHelp(stream);
        } else {
            args = CoordinatorAdminUtils.copyArrayAddFirst(args, subCmd);
            printHelp(stream);
        }
    }

    /**
     * get command
     */
    public static class SubCommandGet extends AbstractAdminCommand {

        private static final String OPT_VERBOSE = "verbose";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            CoordinatorAdminUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            CoordinatorAdminUtils.acceptsStoreMultiple(parser);
            parser.accepts(OPT_VERBOSE, "print store client config without folding same ones");
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  get - Get the store client config for the list of stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  get -s <store-name-list> -u <coordinator-url-list> [--verbose]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and executes the command.
         * 
         * @param args Command-line input
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> storeNames = null;
            List<String> urls = null;
            Boolean verbose = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(CoordinatorAdminUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            CoordinatorAdminUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            CoordinatorAdminUtils.checkRequired(options, CoordinatorAdminUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_STORE);
            if(options.has(OPT_VERBOSE)) {
                verbose = true;
            }

            // execute command
            CoordinatorAdminClient client = new CoordinatorAdminClient();
            Map<String, Map<String, String>> mapUrlToStoreToConfig = Maps.newHashMap();
            for(String url: urls) {
                Map<String, String> mapStoreToConfig = client.getStoreClientConfigMap(storeNames,
                                                                                      url);
                mapUrlToStoreToConfig.put(url, mapStoreToConfig);
            }
            for(String storeName: storeNames) {
                System.out.println("Config for store \"" + storeName + "\"");
                if(verbose) {
                    for(String url: urls) {
                        String config = mapUrlToStoreToConfig.get(url).get(storeName);
                        System.out.println("  on host " + url + ":");
                        System.out.println(config);
                    }
                } else {
                    Map<String, List<String>> mapConfigToUrls = Maps.newHashMap();
                    for(String url: urls) {
                        String config = mapUrlToStoreToConfig.get(url).get(storeName);
                        if(!mapConfigToUrls.containsKey(config)) {
                            mapConfigToUrls.put(config, new ArrayList<String>());
                        }
                        mapConfigToUrls.get(config).add(url);
                    }
                    if(!mapConfigToUrls.isEmpty()) {
                        Set<String> configs = mapConfigToUrls.keySet();
                        for(String config: configs) {
                            List<String> urlList = mapConfigToUrls.get(config);
                            for(String url: urlList) {
                                System.out.println("- on host " + url + ":");
                            }
                            System.out.println(config);
                        }
                    }
                }
                System.out.println();
            }
        }
    }

    /**
     * put command
     */
    public static class SubCommandPut extends AbstractAdminCommand {

        public static final String OPT_D = "d";
        public static final String OPT_DEFINITION = "definition";
        public static final String OPT_F = "f";
        public static final String OPT_FILE = "file";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            CoordinatorAdminUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            parser.acceptsAll(Arrays.asList(OPT_D, OPT_DEFINITION), "config definition string")
                  .withRequiredArg()
                  .describedAs("store-config-definition-string")
                  .ofType(String.class);
            parser.acceptsAll(Arrays.asList(OPT_F, OPT_FILE), "config file path")
                  .withRequiredArg()
                  .describedAs("store-config-file-path")
                  .ofType(String.class);
            // optional options
            CoordinatorAdminUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  put - Put store config to coordinators");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  put -u <url-list> (-d <store-config-def-string> | -f <store-config-file-path>)");
            stream.println("      [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and executes the command.
         * 
         * @param args Command-line input
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> urls = null;
            String storeConfigAvro = null;
            String storeConfigPath = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(CoordinatorAdminUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            CoordinatorAdminUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            CoordinatorAdminUtils.checkRequired(options, OPT_DEFINITION, OPT_FILE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            if(options.has(OPT_DEFINITION)) {
                storeConfigAvro = (String) options.valueOf(OPT_DEFINITION);
            }
            if(options.has(OPT_FILE)) {
                storeConfigPath = (String) options.valueOf(OPT_FILE);
                File storeConfigFile = new File(storeConfigPath);
                if(!storeConfigFile.exists()) {
                    throw new VoldemortException("cannot find config file: '" + storeConfigPath
                                                 + "'.");
                }
                storeConfigAvro = Joiner.on(" ")
                                        .join(IOUtils.readLines(new FileReader(storeConfigFile)))
                                        .trim();
            }
            if(options.has(CoordinatorAdminUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            if(!CoordinatorAdminUtils.askConfirm(confirm, "put store config to coordinators")) {
                return;
            }

            // execute command
            CoordinatorAdminClient client = new CoordinatorAdminClient();
            Boolean succeedAll = true;

            // define coordinator-admin-client here
            for(String url: urls) {
                succeedAll = succeedAll && client.putStoreClientConfigString(storeConfigAvro, url);
            }

            if(!succeedAll) {
                System.out.println("Failed to put config avro to some coordinators.");
            } else {
                System.out.println("Successfully put config to all coordinators.");
            }
        }
    }

    /**
     * delete command
     */
    public static class SubCommandDelete extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            CoordinatorAdminUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            CoordinatorAdminUtils.acceptsStoreMultiple(parser);
            // optional options
            CoordinatorAdminUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  delete - Delete store config on coordinators");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  delete -u <url-list> -s <store-name-list>");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and executes the command.
         * 
         * @param args Command-line input
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> urls = null;
            List<String> storeNames = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(CoordinatorAdminUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            CoordinatorAdminUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            CoordinatorAdminUtils.checkRequired(options, CoordinatorAdminUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_STORE);

            if(options.has(CoordinatorAdminUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            if(!CoordinatorAdminUtils.askConfirm(confirm, "delete store config on coordinators")) {
                return;
            }

            // execute command
            CoordinatorAdminClient client = new CoordinatorAdminClient();
            Boolean succeedAll = true;

            for(String url: urls) {
                succeedAll = succeedAll && client.deleteStoreClientConfig(storeNames, url);
            }

            if(!succeedAll) {
                System.out.println("Failed to delete config avro on some coordinators.");
            } else {
                System.out.println("Successfully deleted config on all coordinators.");
            }
        }
    }
}

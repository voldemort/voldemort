package voldemort.restclient.admin;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;
import voldemort.tools.admin.command.AbstractAdminCommand;

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
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("get")) {
            SubCommandGet.executeCommand(args);
        } else if(subCmd.equals("put-one")) {
            SubCommandPutOne.executeCommand(args);
        } else if(subCmd.equals("put-many")) {
            SubCommandPutMany.executeCommand(args);
        } else if(subCmd.equals("put-all")) {
            SubCommandPutAll.executeCommand(args);
        } else if(subCmd.equals("delete")) {
            SubCommandDelete.executeCommand(args);
        } else {
            args = AdminToolUtils.copyArrayAddFirst(args, subCmd);
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
        stream.println("get                    ?");
        stream.println("put-one                ?");
        stream.println("put-mant               ?");
        stream.println("put-all                ?");
        stream.println("delete                 ?");
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
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("get")) {
            SubCommandGet.printHelp(stream);
        } else if(subCmd.equals("put-one")) {
            SubCommandPutOne.printHelp(stream);
        } else if(subCmd.equals("put-many")) {
            SubCommandPutMany.printHelp(stream);
        } else if(subCmd.equals("put-all")) {
            SubCommandPutAll.printHelp(stream);
        } else if(subCmd.equals("delete")) {
            SubCommandDelete.printHelp(stream);
        } else {
            args = AdminToolUtils.copyArrayAddFirst(args, subCmd);
            printHelp(stream);
        }
    }

    /**
     * get command
     */
    public static class SubCommandGet extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  get - ?");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  get -u <url-list> -s <store-name-list>");
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

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);

            // execute command
            // define coordinator-admin-client here

            // execute coordinator-admin-get command with arguments
        }
    }

    /**
     * put-one command
     */
    public static class SubCommandPutOne extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  get - ?");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  get -u <url-list> -s <store-name-list>");
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

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);

            // execute command
            // define coordinator-admin-client here

            // execute coordinator-admin-get command with arguments
        }
    }

    /**
     * put-many command
     */
    public static class SubCommandPutMany extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  get - ?");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  get -u <url-list> -s <store-name-list>");
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

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);

            // execute command
            // define coordinator-admin-client here

            // execute coordinator-admin-get command with arguments
        }
    }

    /**
     * put-all command
     */
    public static class SubCommandPutAll extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  get - ?");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  get -u <url-list> -s <store-name-list>");
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

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);

            // execute command
            // define coordinator-admin-client here

            // execute coordinator-admin-get command with arguments
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
            AdminParserUtils.acceptsHelp(parser);
            // required options
            CoordinatorAdminUtils.acceptsUrlMultiple(parser);
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  get - ?");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  get -u <url-list> -s <store-name-list>");
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

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, CoordinatorAdminUtils.OPT_URL);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);

            // load parameters
            urls = (List<String>) options.valuesOf(CoordinatorAdminUtils.OPT_URL);
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);

            // execute command
            // define coordinator-admin-client here

            // execute coordinator-admin-get command with arguments
        }
    }
}

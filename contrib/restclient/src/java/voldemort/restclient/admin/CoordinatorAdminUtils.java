package voldemort.restclient.admin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

public class CoordinatorAdminUtils {

    // options without argument
    public static final String OPT_CONFIRM = "confirm";
    public static final String OPT_H = "h";
    public static final String OPT_HELP = "help";

    // options with multiple arguments
    public static final String OPT_S = "s";
    public static final String OPT_STORE = "store";
    public static final String OPT_U = "u";
    public static final String OPT_URL = "url";

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
     * Adds OPT_S | OPT_STORE option to OptionParser, with one argument.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsStoreMultiple(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_S, OPT_STORE), "store name list")
              .withRequiredArg()
              .describedAs("store-name-list")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
    }

    /**
     * Adds OPT_U | OPT_URL option to OptionParser, with multiple arguments.
     * 
     * @param parser OptionParser to be modified
     * @param required Tells if this option is required or optional
     */
    public static void acceptsUrlMultiple(OptionParser parser) {
        parser.acceptsAll(Arrays.asList(OPT_U, OPT_URL), "coordinator bootstrap urls")
              .withRequiredArg()
              .describedAs("url-list")
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
     * Checks if there's exactly one option that exists among all possible opts.
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
     * Checks if there's exactly one option that exists among all possible opts.
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
     * Checks if there's exactly one option that exists among all opts.
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
     * Checks if there's at most one option that exists among all opts.
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
     * Checks if there's at most one option that exists among all opts.
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
     * Checks if there's at most one option that exists among all opts.
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

    /**
     * Utility function that pauses and asks for confirmation on dangerous
     * operations.
     * 
     * @param confirm User has already confirmed in command-line input
     * @param opDesc Description of the dangerous operation
     * @throws IOException
     * @return True if user confirms the operation in either command-line input
     *         or here.
     * 
     */
    public static Boolean askConfirm(Boolean confirm, String opDesc) throws IOException {
        if(confirm) {
            System.out.println("Confirmed " + opDesc + " in command-line.");
            return true;
        } else {
            System.out.println("Are you sure you want to " + opDesc + "? (yes/no)");
            BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
            String text = buffer.readLine();
            return text.equals("yes");
        }
    }

    /**
     * Utility function that copies a string array except for the first element
     * 
     * @param arr Original array of strings
     * @return Copied array of strings
     */
    public static String[] copyArrayCutFirst(String[] arr) {
        if(arr.length > 1) {
            String[] arrCopy = new String[arr.length - 1];
            System.arraycopy(arr, 1, arrCopy, 0, arrCopy.length);
            return arrCopy;
        } else {
            return new String[0];
        }
    }

    /**
     * Utility function that copies a string array and add another string to
     * first
     * 
     * @param arr Original array of strings
     * @param add
     * @return Copied array of strings
     */
    public static String[] copyArrayAddFirst(String[] arr, String add) {
        String[] arrCopy = new String[arr.length + 1];
        arrCopy[0] = add;
        System.arraycopy(arr, 0, arrCopy, 1, arr.length);
        return arrCopy;
    }
}

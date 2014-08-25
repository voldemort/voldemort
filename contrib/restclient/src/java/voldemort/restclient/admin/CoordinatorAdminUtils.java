package voldemort.restclient.admin;

import java.util.Arrays;

import joptsimple.OptionParser;

public class CoordinatorAdminUtils {

    public static final String OPT_U = "u";
    public static final String OPT_URL = "url";

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
}

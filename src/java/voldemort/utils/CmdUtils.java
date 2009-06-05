package voldemort.utils;

import java.util.HashSet;
import java.util.Set;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Helper functions for command line parsing. Because jopt-simple is a little
 * too simple.
 * 
 * @author jay
 * 
 */
public class CmdUtils {

    public static Set<String> missing(OptionSet options, String... required) {
        Set<String> missing = new HashSet<String>();
        for(String arg: required)
            if(!options.has(arg))
                missing.add(arg);
        return missing;
    }

    public static Set<OptionSpec<?>> missing(OptionSet options, OptionSpec<?>... required) {
        Set<OptionSpec<?>> missing = new HashSet<OptionSpec<?>>();
        for(OptionSpec<?> opt: required)
            if(!options.has(opt))
                missing.add(opt);
        return missing;
    }

    @SuppressWarnings("unchecked")
    public static <T> T valueOf(OptionSet options, String opt, T defaultValue) {
        if(options.has(opt))
            return (T) options.valueOf(opt);
        else
            return defaultValue;
    }

    public static <T> T valueOf(OptionSet options, OptionSpec<T> opt, T defaultValue) {
        if(options.has(opt))
            return options.valueOf(opt);
        else
            return defaultValue;
    }

}

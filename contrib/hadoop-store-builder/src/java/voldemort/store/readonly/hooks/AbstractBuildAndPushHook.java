package voldemort.store.readonly.hooks;

import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Base class for some useful facilities for any hooks.
 *
 * Not mandatory to extend in order to use BnP hooks...
 */
abstract public class AbstractBuildAndPushHook implements BuildAndPushHook {

    protected final String thisClassName = this.getClass().getName();

    // Recommended namespace for hook-specific configs
    protected final String configKeyPrefix = "hooks." + thisClassName + ".";

    // Logging
    protected final Logger log = Logger.getLogger(thisClassName);
}

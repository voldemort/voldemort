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

    // Utility functions for common config handling operations.

    protected Integer getIntPropertyOrFail(Properties properties, String propertyName) {
        return getIntProperty(properties, propertyName, null);
    }

    protected Integer getIntProperty(Properties properties, String propertyName, String defaultValue) {
        try {
            return Integer.parseInt(getStringProperty(properties, propertyName, defaultValue));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("A valid integer must be provided for the config key '" +
                    propertyName + "'. The " + getName() + " will not run.", e);
        }
    }

    protected String getStringPropertyOrFail(Properties properties, String propertyName) {
        return getStringProperty(properties, propertyName, null);
    }

    protected String getStringProperty(Properties properties, String propertyName, String defaultValue) {
        String propertyValue = properties.getProperty(propertyName, defaultValue);
        if (propertyValue == null || propertyValue.isEmpty()) {
            throw new IllegalArgumentException("A value must be provided for the config key '" +
                    propertyName + "'. The " + getName() + " will not run.");
        }
        return propertyValue;
    }
}

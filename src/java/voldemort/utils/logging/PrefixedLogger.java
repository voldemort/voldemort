package voldemort.utils.logging;

import org.apache.log4j.Logger;

/**
 * A logger implementation which prepends a string to all of its logs.
 */
public class PrefixedLogger extends Logger {
    private static final String UNIQUE_NAME_PREFIX_SEPARATOR = "|";
    private final String prefix;
    protected PrefixedLogger(String name, String prefix) {
        super(name);
        this.prefix = prefix;
    }
    public static Logger getLogger(String name, String prefix) {
        return Logger.getLogger(name + UNIQUE_NAME_PREFIX_SEPARATOR + prefix,
                                new PrefixedLoggerFactory(prefix));
    }
    private String generateMessage(Object message) {
        return prefix + " : " + message;
    }
    @Override
    public void error(Object message, Throwable throwable) {
        super.error(generateMessage(message), throwable);
    }
    @Override
    public void error(Object message) {
        super.error(generateMessage(message));
    }
    @Override
    public void info(Object message, Throwable throwable) {
        super.info(generateMessage(message), throwable);
    }
    @Override
    public void info(Object message) {
        super.info(generateMessage(message));
    }
    @Override
    public void warn(Object message, Throwable throwable) {
        super.warn(generateMessage(message), throwable);
    }
    @Override
    public void warn(Object message) {
        super.warn(generateMessage(message));
    }
    @Override
    public void debug(Object message, Throwable throwable) {
        super.debug(generateMessage(message), throwable);
    }
    @Override
    public void debug(Object message) {
        super.debug(generateMessage(message));
    }
    @Override
    public void trace(Object message, Throwable throwable) {
        super.trace(generateMessage(message), throwable);
    }
    @Override
    public void trace(Object message) {
        super.trace(generateMessage(message));
    }
    @Override
    public void fatal(Object message, Throwable throwable) {
        super.fatal(generateMessage(message), throwable);
    }
    @Override
    public void fatal(Object message) {
        super.fatal(generateMessage(message));
    }
}

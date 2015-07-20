package voldemort.utils.logging;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

/**
 * Used to pass into {@link Logger#getLogger(String, org.apache.log4j.spi.LoggerFactory)}
 * in order to get an instance of {@link voldemort.utils.logging.PrefixedLogger}.
 */
public class PrefixedLoggerFactory implements LoggerFactory {
    private final String prefix;
    PrefixedLoggerFactory(String prefix) {
        this.prefix = prefix;
    }
    @Override
    public Logger makeNewLoggerInstance(String name) {
        return new PrefixedLogger(name, prefix);
    }
}

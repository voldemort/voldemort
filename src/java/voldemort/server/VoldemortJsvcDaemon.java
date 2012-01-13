package voldemort.server;

import static voldemort.utils.Utils.croak;

import org.apache.log4j.Logger;

/**
 * Daemon class for use with the jsvc wrapper.
 * <p>
 * Simple example usage:
 * <p>
 * 
 * <pre>
 *  jsvc -pidfile ./voldemort.pid voldemort.server.VoldemortJsvcDaemon
 * </pre>
 * <p>
 * The above assumes that JAVA_HOME, CLASSPATH, and VOLDEMORT_HOME have all been
 * exported.
 * <p>
 * A more detailed example might be:
 * 
 * <pre>
 * jsvc \
 *   -Xmx2G \
 *   -pidfile /var/run/voldemort.pid \
 *   -errfile &quot;&amp;1&quot; \
 *   -outfile /var/log/voldemort.out \
 *   -user voldemort \
 *   -Dcom.sun.management.jmxremote \
 *   -Dlog4j.configuration=file:///etc/voldemort/log4j.properties \
 *   voldemort.server.VoldemortJsvcDaemon /etc/voldemort
 * </pre>
 * 
 * See <a href="http://commons.apache.org/daemon/jsvc.html">Commons Daemon</a>
 * documentation for more info.
 * 
 * 
 */
public class VoldemortJsvcDaemon {

    private static final Logger logger = Logger.getLogger(VoldemortServer.class.getName());
    private VoldemortServer server = null;

    public void init(String[] args) throws Exception {
        VoldemortConfig config = null;
        try {
            if(args.length == 0)
                config = VoldemortConfig.loadFromEnvironmentVariable();
            else if(args.length == 1)
                config = VoldemortConfig.loadFromVoldemortHome(args[0]);
            else if(args.length == 2)
                config = VoldemortConfig.loadFromVoldemortHome(args[0], args[1]);
            else
                croak("USAGE: java " + VoldemortJsvcDaemon.class.getName()
                      + " [voldemort_home_dir] [voldemort_config_dir]");
        } catch(Exception e) {
            logger.error(e);
            croak("Error while loading configuration: " + e.getMessage());
        }

        server = new VoldemortServer(config);
    }

    public void start() {
        if(!server.isStarted())
            server.start();
    }

    public void stop() {
        if(server.isStarted())
            server.stop();
    }

    public void destroy() {

    }
}

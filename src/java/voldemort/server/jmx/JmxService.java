package voldemort.server.jmx;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Cluster;
import voldemort.server.AbstractService;
import voldemort.server.VoldemortServer;
import voldemort.server.VoldemortService;
import voldemort.store.Store;
import voldemort.utils.JmxUtils;

/**
 * A service that manages JMX registration
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "The JMX service")
public class JmxService extends AbstractService {

    private final Logger logger = Logger.getLogger(JmxService.class);

    private final MBeanServer mbeanServer;
    private final VoldemortServer server;
    private final Cluster cluster;
    private final List<VoldemortService> services;
    private final Set<ObjectName> registeredBeans;
    private final Map<String, Store<byte[], byte[]>> storeMap;

    public JmxService(String name,
                      VoldemortServer server,
                      Cluster cluster,
                      Map<String, Store<byte[], byte[]>> storeMap,
                      List<VoldemortService> services) {
        super(name);
        this.server = server;
        this.cluster = cluster;
        this.services = services;
        this.mbeanServer = ManagementFactory.getPlatformMBeanServer();
        this.registeredBeans = new HashSet<ObjectName>();
        this.storeMap = storeMap;
    }

    @Override
    protected void startInner() {
        registerBean(server, JmxUtils.createObjectName(VoldemortServer.class));
        registerBean(cluster, JmxUtils.createObjectName(Cluster.class));
        for (VoldemortService service : services) {
            logger.debug("Registering mbean for service '" + service.getName() + "'.");
            registerBean(service, JmxUtils.createObjectName(service.getClass()));
        }
        for (Store<byte[], byte[]> store : storeMap.values()) {
            logger.info("Registering mbean for store '" + store.getName() + "'.");
            registerBean(store,
                         JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                   store.getName()));
        }
    }

    @Override
    protected void stopInner() {
        for (ObjectName name : registeredBeans)
            JmxUtils.unregisterMbean(mbeanServer, name);
        registeredBeans.clear();
    }

    private void registerBean(Object o, ObjectName name) {
        synchronized (registeredBeans) {
            if (mbeanServer.isRegistered(name)) {
                logger.warn("Overwriting mbean " + name);
                JmxUtils.unregisterMbean(mbeanServer, name);
            }
            JmxUtils.registerMbean(mbeanServer, JmxUtils.createModelMBean(o), name);
            this.registeredBeans.add(name);
        }
    }

}

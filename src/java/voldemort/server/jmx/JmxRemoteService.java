package voldemort.server.jmx;
/*
 * Copyright 2008-2009 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


import org.apache.log4j.Logger;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;

/**
 * This listener fixes the port used by JMX/RMI Server making things much simpler if you need to connect JConsole or
 * similar to a remote Tomcat instance that is running behind a firewall.
 * Only the ports are configured via the listener. The remainder of the configuration is via the standard
 * system properties for configuring JMX.
 * <p/>
 * Adapted from org.apache.catalina.mbeans.JmxRemoteLifecycleListener in the Apache Tomcat project.
 */
public class JmxRemoteService extends AbstractService {

    private final Logger log = Logger.getLogger(JmxService.class);

    private int rmiRegistryPort;
    private int rmiServerPort;

    protected boolean rmiSSL = true;
    protected String ciphers[] = null;
    protected String protocols[] = null;
    protected boolean clientAuth = true;
    protected boolean authenticate = true;
    protected String passwordFile = null;
    protected String accessFile = null;
    protected boolean useLocalPorts = false;

    protected JMXConnectorServer jmxConnector = null;

    public JmxRemoteService(int rmiRegistryPort, int rmiServerPort) {
        super(ServiceType.JMX_REMOTE);
        this.rmiRegistryPort = rmiRegistryPort;
        this.rmiServerPort = rmiServerPort;
    }


    private void init() {
        // Get all the other parameters required from the standard system
        // properties. Only need to get the parameters that affect the creation
        // of the server port.
        String rmiSSLValue = System.getProperty(
                "com.sun.management.jmxremote.ssl", "false");
        rmiSSL = Boolean.parseBoolean(rmiSSLValue);

        String protocolsValue = System.getProperty(
                "com.sun.management.jmxremote.ssl.enabled.protocols");
        if (protocolsValue != null) {
            protocols = protocolsValue.split(",");
        }

        String ciphersValue = System.getProperty(
                "com.sun.management.jmxremote.ssl.enabled.cipher.suites");
        if (ciphersValue != null) {
            ciphers = ciphersValue.split(",");
        }

        String clientAuthValue = System.getProperty(
                "com.sun.management.jmxremote.ssl.need.client.auth", "false");
        clientAuth = Boolean.parseBoolean(clientAuthValue);

        String authenticateValue = System.getProperty(
                "com.sun.management.jmxremote.authenticate", "false");
        authenticate = Boolean.parseBoolean(authenticateValue);

        passwordFile = System.getProperty(
                "com.sun.management.jmxremote.password.file",
                "jmxremote.password");

        accessFile = System.getProperty(
                "com.sun.management.jmxremote.access.file",
                "jmxremote.access");
    }


    @Override
    protected void startInner() {
        // Configure using standard jmx system properties
        init();

        // Prevent an attacker guessing the RMI object ID
        System.setProperty("java.rmi.server.randomIDs", "true");

        // Create the environment
        HashMap<String, Object> env = new HashMap<String, Object>();

        RMIClientSocketFactory csf = null;
        RMIServerSocketFactory ssf = null;

        // Configure SSL for RMI connection if required
        if (rmiSSL) {
            csf = new SslRMIClientSocketFactory();
            ssf = new SslRMIServerSocketFactory(ciphers, protocols,
                    clientAuth);
        }

        // Force the use of local ports if required
        if (useLocalPorts) {
            csf = new RmiClientLocalhostSocketFactory(csf);
        }

        // Populate the env properties used to create the server
        if (csf != null) {
            env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                    csf);
        }
        if (ssf != null) {
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                    ssf);
        }


        // Configure authentication
        if (authenticate) {
            env.put("jmx.remote.x.password.file", passwordFile);
            env.put("jmx.remote.x.access.file", accessFile);
        }

        // Create the Platform server
        jmxConnector = createServer(rmiRegistryPort,
                rmiServerPort, env,
                ManagementFactory.getPlatformMBeanServer());

    }

    @Override
    protected void stopInner() {
        // When the server starts, configure JMX/RMI
        destroyServer(jmxConnector);
    }


    private JMXConnectorServer createServer(int theRmiRegistryPort, int theRmiServerPort,
                                            HashMap<String, Object> theEnv, MBeanServer theMBeanServer) {

        // Create the RMI registry
        try {
            LocateRegistry.createRegistry(theRmiRegistryPort);
        } catch (RemoteException e) {
            log.error(
                    "Unable to create RMI registry at " + theRmiRegistryPort, e);
            return null;
        }

        // Build the connection string with fixed ports
        StringBuilder url = new StringBuilder();
        url.append("service:jmx:rmi://localhost:");
        url.append(theRmiServerPort);
        url.append("/jndi/rmi://localhost:");
        url.append(theRmiRegistryPort);
        url.append("/jmxrmi");
        JMXServiceURL serviceUrl;
        try {
            serviceUrl = new JMXServiceURL(url.toString());
        } catch (MalformedURLException e) {
            log.error("Invalid service URL: " + url.toString(), e);
            return null;
        }

        // Start the JMX server with the connection string
        JMXConnectorServer cs = null;
        try {
            cs = JMXConnectorServerFactory.newJMXConnectorServer(
                    serviceUrl, theEnv, theMBeanServer);
            cs.start();
            log.info("Started JMX server at RMI registry port " + theRmiRegistryPort + " and RMI server port " + theRmiServerPort);
        } catch (IOException e) {
            log.error("Unable to start JMX server", e);
        }
        return cs;
    }

    private void destroyServer(JMXConnectorServer theConnectorServer) {
        if (theConnectorServer != null) {
            try {
                theConnectorServer.stop();
            } catch (IOException e) {
                log.error("Unable to stop JMX server", e);
            }
        }
    }

    public static class RmiClientLocalhostSocketFactory
            implements RMIClientSocketFactory, Serializable {
        private static final long serialVersionUID = 1L;

        private static final String FORCED_HOST = "localhost";

        private RMIClientSocketFactory factory = null;

        public RmiClientLocalhostSocketFactory(RMIClientSocketFactory theFactory) {
            factory = theFactory;
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            if (factory == null) {
                return new Socket(FORCED_HOST, port);
            } else {
                return factory.createSocket(FORCED_HOST, port);
            }
        }


    }

}

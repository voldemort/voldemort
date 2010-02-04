package voldemort.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.xml.ClusterMapper;

/**
 * Helper function to start/stop manage a Voldemort Server in a JVM.
 * 
 * @author bbansal
 * 
 */
public class ServerJVMTestUtils {

    public static Process startServerJVM(Node node, String voldemortHome) throws IOException {
        List<String> env = Arrays.asList("CLASSPATH=" + System.getProperty("java.class.path"));

        String command = "java  voldemort.server.VoldemortServer " + voldemortHome;
        // System.out.println("command:" + command + " env:" + env);
        Process process = Runtime.getRuntime().exec(command, env.toArray(new String[0]));
        ServerTestUtils.waitForServerStart(node);
        startOutputErrorConsumption(process);
        return process;
    }

    public static void startOutputErrorConsumption(final Process process) {
        final InputStream io = new BufferedInputStream(process.getInputStream());
        new Thread(new Runnable() {

            public void run() {
                while(true) {
                    try {
                        process.exitValue();
                        try {
                            io.close();
                        } catch(IOException e) {
                            e.printStackTrace();
                        }
                        return;
                    } catch(IllegalThreadStateException e) {
                        // still running
                        StringBuffer buffer = new StringBuffer();
                        try {
                            int c;
                            while((c = io.read()) != -1) {
                                buffer.append((char) c);
                            }
                        } catch(Exception e1) {
                            return;
                        } finally {
                            System.out.println(buffer.toString());
                        }
                    }
                }
            }
        }).start();
    }

    public static void StopServerJVM(Process server) {
        System.out.println("killing process" + server);
        server.destroy();

        try {
            server.waitFor();
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static String createAndInitializeVoldemortHome(boolean useNio,
                                                          int node,
                                                          String storesXmlfile,
                                                          Cluster cluster) throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfig(useNio,
                                                                    node,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile,
                                                                    new Properties());

        // Initialize voldemort config dir with all required files.
        // cluster.xml
        File clusterXml = new File(config.getMetadataDirectory() + File.separator + "cluster.xml");
        FileUtils.writeStringToFile(clusterXml, new ClusterMapper().writeCluster(cluster));

        // stores.xml
        File storesXml = new File(config.getMetadataDirectory() + File.separator + "stores.xml");
        FileUtils.copyFile(new File(storesXmlfile), storesXml);

        // server.properties
        File serverProperties = new File(config.getMetadataDirectory() + File.separator
                                         + "server.properties");
        FileUtils.writeLines(serverProperties, Arrays.asList("node.id=" + node,
                                                             "bdb.cache.size=" + 1024 * 1024,
                                                             "enable.metadata.checking=" + false,
                                                             "enable.network.classloader=" + false));

        return config.getVoldemortHome();
    }
}

package voldemort;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.httpclient.HttpClient;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import voldemort.server.http.StoreServlet;
import voldemort.server.socket.SocketServer;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.versioning.Versioned;

/**
 * Helper functions for testing with real server implementations
 * 
 * @author jay
 *
 */
public class ServerTestUtils {
    
    public static ConcurrentMap<String, Store<byte[],byte[]>> getStores(String storeName, 
                                                                        String clusterXml, 
                                                                        String storesXml) {
        ConcurrentMap<String, Store<byte[],byte[]>> stores = 
            new ConcurrentHashMap<String,Store<byte[],byte[]>>(1);
        stores.put(storeName, new InMemoryStorageEngine<byte[],byte[]>(storeName));
        Store<String,String> metadataInner = new InMemoryStorageEngine<String,String>("metadata");
        metadataInner.put("cluster.xml", new Versioned<String>(clusterXml));
        metadataInner.put("stores.xml", new Versioned<String>(storesXml));
        MetadataStore metadata = new MetadataStore(metadataInner, 
                                                   stores);
        stores.put(MetadataStore.METADATA_STORE_NAME, metadata);
        return stores;
    }

    public static SocketServer getSocketServer(String clusterXml, 
                                               String storesXml, 
                                               String storeName, 
                                               int port) {

        SocketServer socketServer = new SocketServer(getStores(storeName, clusterXml, storesXml), port, 5, 10);
        socketServer.start();
        socketServer.awaitStartupCompletion();
        return socketServer;
    }
    
    public static SocketStore getSocketStore(String storeName, int port) {
        SocketPool socketPool = new SocketPool(1, 2, 1000);
        return new SocketStore(storeName, "localhost", port, socketPool);
    }
    
    public static Context getJettyServer(String clusterXml, 
                                         String storesXml,
                                         String storeName, 
                                         int port) throws Exception {
        ConcurrentMap<String, Store<byte[],byte[]>> stores = getStores(storeName, clusterXml, storesXml);
        
        // initialize servlet
        Server server = new Server(port);
        server.setSendServerVersion(false);
        Context context = new Context(server, "/", Context.NO_SESSIONS);
        
        context.addServlet(new ServletHolder(new StoreServlet(stores)), "/*");
        server.start();
        return context;
    }
    
    public static HttpStore getHttpStore(String storeName, int port) {
        return new HttpStore(storeName, "localhost", port, new HttpClient());
    }
}

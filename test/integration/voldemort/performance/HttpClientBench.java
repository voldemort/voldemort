package voldemort.performance;

import java.util.concurrent.*;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.*;

public class HttpClientBench {
    
    private static final int DEFAULT_CONNECTION_MANAGER_TIMEOUT = 100000;
    private static final int DEFAULT_MAX_CONNECTIONS = 10;
    private static final int DEFAULT_MAX_HOST_CONNECTIONS = 10;
    private static final String VOLDEMORT_USER_AGENT = "vldmrt/0.01";
    
    public static void main(String[] args) throws Exception {
        if(args.length < 3) {
            System.err.println("USAGE: java HttpClientBench url numThreads numRequests");
            System.exit(1);
        }
        
        final String url = args[0];
        final int numThreads = Integer.parseInt(args[1]);
        final int numRequests = Integer.parseInt(args[2]);
        
        System.out.println("Benchmarking against " + url);
        
        final HttpClient client = createClient();
        PerformanceTest perfTest = new PerformanceTest() {
            public void doOperation(int index) {
                GetMethod get = new GetMethod(url);
                try {
                    client.executeMethod(get);
                    byte[] bytes = get.getResponseBody();
                } catch(Exception e) {
                    e.printStackTrace();
                } finally {
                    get.releaseConnection();
                }
            }
        };
        perfTest.run(numRequests, numThreads);
        perfTest.printStats();

        System.exit(1);
    }
    
    private static HttpClient createClient() {
        HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        HttpClient httpClient = new HttpClient(connectionManager);
        HttpClientParams clientParams = httpClient.getParams();
        clientParams.setConnectionManagerTimeout(DEFAULT_CONNECTION_MANAGER_TIMEOUT);
        clientParams.setSoTimeout(500);
        clientParams.setParameter(HttpClientParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(0, false));
        clientParams.setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        clientParams.setBooleanParameter("http.tcp.nodelay", false);
        clientParams.setIntParameter("http.socket.receivebuffer", 60000);
        clientParams.setParameter("http.useragent", VOLDEMORT_USER_AGENT);
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.setHost("localhost");
        hostConfig.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
        httpClient.setHostConfiguration(hostConfig);
        HttpConnectionManagerParams managerParams = httpClient.getHttpConnectionManager().getParams();
        managerParams.setConnectionTimeout(DEFAULT_CONNECTION_MANAGER_TIMEOUT);
        managerParams.setMaxTotalConnections(DEFAULT_MAX_CONNECTIONS);
        managerParams.setMaxConnectionsPerHost(httpClient.getHostConfiguration(), DEFAULT_MAX_HOST_CONNECTIONS);
        managerParams.setStaleCheckingEnabled(false);
        
        return httpClient;
    }

}

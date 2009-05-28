package voldemort.server.http.gui;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.VoldemortException;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.server.socket.SocketService;
import voldemort.store.Store;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

/**
 * Servlet to return status and stats information about the server and the
 * various stores.
 * 
 * Currently mapped as /server-status, no parameters return HTML
 * 
 * /server-status?format=json returns JSON outupt
 * 
 * /server-status?action=reset&store=<storename> resets the stats for a given
 * store
 * 
 */
public class StatusServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VoldemortServer server;
    private VelocityEngine velocityEngine;
    private SocketService socketService;

    private String myMachine;

    /* For use by servlet container */
    public StatusServlet() {}

    public StatusServlet(VoldemortServer server, VelocityEngine engine) {
        this.server = Utils.notNull(server);
        this.velocityEngine = Utils.notNull(engine);
        this.socketService = (SocketService) server.getService(ServiceType.SOCKET);
        try {
            this.myMachine = InetAddress.getLocalHost().getHostName();
        } catch(UnknownHostException e) {
            myMachine = "unknown";
        }
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.server = (VoldemortServer) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY));
        this.velocityEngine = (VelocityEngine) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        if("reset".equals(request.getParameter("action"))) {
            String storeName = request.getParameter("store");

            if(storeName != null) {
                Store<ByteArray, byte[]> store = server.getStoreRepository()
                                                       .getLocalStore(storeName);

                if(store != null && store instanceof StatTrackingStore<?, ?>) {
                    ((StatTrackingStore<?, ?>) store).resetStatistics();
                }
            }
        }

        String format = request.getParameter("format");

        if("json".equals(format)) {
            outputJSON(request, response);
            return;
        }

        Map<String, Object> params = Maps.newHashMap();
        params.put("status", socketService.getStatusManager());
        params.put("counters", Tracked.values());
        params.put("stores", server.getStoreRepository().getAllLocalStores());
        velocityEngine.render("status.vm", params, response.getOutputStream());
    }

    protected void outputJSON(HttpServletRequest request, HttpServletResponse response) {
        StringBuilder sb = new StringBuilder("{\n");

        sb.append("  \"requestURI\": \"");
        sb.append(request.getRequestURI());
        sb.append("\",");
        sb.append("\n  \"servertime\": \"");
        sb.append(new Date());
        sb.append("\",");

        sb.append("\n  \"server\": \"");
        sb.append(myMachine);
        sb.append("\",");

        sb.append("\n  \"uptime\": \"");
        sb.append(socketService.getStatusManager().getFormattedUptime());
        sb.append("\",");

        sb.append("\n  \"num_workers\": ");
        sb.append(socketService.getStatusManager().getActiveWorkersCount());
        sb.append(",");

        sb.append("\n  \"pool_size\": ");
        sb.append(socketService.getStatusManager().getWorkerPoolSize());
        sb.append(",");

        sb.append("\n  \"storestats\": {");

        for(Store<ByteArray, byte[]> store: server.getStoreRepository().getAllLocalStores()) {

            if(store instanceof StatTrackingStore<?, ?>) {

                StatTrackingStore<?, ?> statStore = (StatTrackingStore<?, ?>) store;

                sb.append("\n    \"");
                sb.append(store.getName());
                sb.append("\" : {\n");

                sb.append("      \"num_get\": ");
                sb.append(statStore.getNumberOfCallsToGet());
                sb.append(",\n");

                sb.append("      \"ave_get_time\": ");
                sb.append(statStore.getAverageGetCompletionTimeInMs());
                sb.append(",\n");

                sb.append("      \"num_getall\": ");
                sb.append(statStore.getNumberOfCallsToGetAll());
                sb.append(",\n");

                sb.append("      \"ave_getall_time\": ");
                sb.append(statStore.getAverageGetAllCompletionTimeInMs());
                sb.append(",\n");

                sb.append("      \"num_put\": ");
                sb.append(statStore.getNumberOfCallsToPut());
                sb.append(",\n");

                sb.append("      \"ave_put_time\": ");
                sb.append(statStore.getAveragePutCompletionTimeInMs());
                sb.append(",\n");

                sb.append("      \"num_delete\": ");
                sb.append(statStore.getNumberOfCallsToDelete());
                sb.append(",\n");

                sb.append("      \"ave_delete_time\": ");
                sb.append(statStore.getAverageDeleteCompletionTimeInMs());
                sb.append(",\n");

                sb.append("    },");
            }
        }

        sb.append("  }\n");
        sb.append("}\n");

        try {
            OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream());
            writer.write(sb.toString());
            writer.flush();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }
}
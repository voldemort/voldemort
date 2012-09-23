package voldemort.server.http.gui;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.VoldemortException;
import voldemort.common.service.ServiceType;
import voldemort.server.AbstractSocketService;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.store.Store;
import voldemort.store.stats.RequestCounter;
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
    private AbstractSocketService abstractSocketService;

    private String myMachine;

    public StatusServlet(VoldemortServer server, VelocityEngine engine) {
        this.server = Utils.notNull(server);
        this.velocityEngine = Utils.notNull(engine);
        this.abstractSocketService = (AbstractSocketService) server.getService(ServiceType.SOCKET);
        try {
            this.myMachine = InetAddress.getLocalHost().getHostName();
        } catch(UnknownHostException e) {
            myMachine = "unknown";
        }
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.server = (VoldemortServer) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.SERVER_KEY));
        this.velocityEngine = (VelocityEngine) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String storeName = request.getParameter("store");

        // TODO: Shouldn't this be done through a POST?
        if("reset".equals(request.getParameter("action"))) {
            if(storeName != null) {
                Store<ByteArray, byte[], byte[]> store = server.getStoreRepository()
                                                               .getLocalStore(storeName);

                if(store != null && store instanceof StatTrackingStore) {
                    ((StatTrackingStore) store).resetStatistics();
                }
            }
        }

        String format = request.getParameter("format");
        if("json".equals(format)) {
            outputJSON(response);
            return;
        } else {
            response.setContentType("text/html");

            long refreshTime = 600;
            String refresh = request.getParameter("refresh");
            if(refresh != null) {
                try {
                    refreshTime = Integer.parseInt(refresh);
                } catch(NumberFormatException e) {
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST);
                }
            }

            List<Store<ByteArray, byte[], byte[]>> stores = null;
            if(storeName == null) {
                stores = server.getStoreRepository().getAllLocalStores();
            } else {
                stores = Collections.singletonList(server.getStoreRepository()
                                                         .getLocalStore(storeName));
            }

            Map<String, Object> params = Maps.newHashMap();
            params.put("status", abstractSocketService.getStatusManager());
            params.put("counters", Tracked.values());
            params.put("stores", stores);
            params.put("refresh", refreshTime);
            velocityEngine.render("status.vm", params, response.getOutputStream());
        }
    }

    protected void outputJSON(HttpServletResponse response) {
        StringBuilder sb = new StringBuilder("{\n");

        sb.append("  \"servertime\": \"");
        sb.append(new Date());
        sb.append("\",");

        sb.append("\n  \"server\": \"");
        sb.append(myMachine);
        sb.append("\",");

        sb.append("\n  \"node\": \"");
        sb.append(server.getMetadataStore().getNodeId());
        sb.append("\",");

        sb.append("\n  \"uptime\": \"");
        sb.append(abstractSocketService.getStatusManager().getFormattedUptime());
        sb.append("\",");

        sb.append("\n  \"num_workers\": ");
        sb.append(abstractSocketService.getStatusManager().getActiveWorkersCount());
        sb.append(",");

        sb.append("\n  \"pool_size\": ");
        sb.append(abstractSocketService.getStatusManager().getWorkerPoolSize());
        sb.append(",");

        sb.append("\n  \"stores\": {");

        int i = 0;
        for(Store<ByteArray, byte[], byte[]> store: server.getStoreRepository().getAllLocalStores()) {

            if(store instanceof StatTrackingStore) {

                StatTrackingStore statStore = (StatTrackingStore) store;

                Map<Tracked, RequestCounter> stats = statStore.getStats().getCounters();

                if(i++ > 0) {
                    sb.append(",");
                }

                sb.append("\n    \"");
                sb.append(statStore.getName());
                sb.append("\" : {\n");

                int j = 0;

                for(Tracked t: Tracked.values()) {

                    if(t == Tracked.EXCEPTION) {
                        continue;
                    }

                    if(j++ > 0) {
                        sb.append(",\n");
                    }

                    sb.append("        \"");
                    sb.append(t.toString());
                    sb.append("\": { ");

                    sb.append("\"total\": ");
                    sb.append(stats.get(t).getTotalCount());
                    sb.append(", ");

                    sb.append("\"operations\": ");
                    sb.append(stats.get(t).getCount());
                    sb.append(", ");

                    sb.append("\"throughput\": ");
                    sb.append(stats.get(t).getDisplayThroughput());
                    sb.append(", ");

                    sb.append("\"avg_time_ms\": ");
                    sb.append(stats.get(t).getDisplayAverageTimeInMs());
                    sb.append(" }");
                }

                sb.append(",\n        \"num_exceptions\": ");
                sb.append(statStore.getStats().getCount(Tracked.EXCEPTION));
                sb.append("\n");

                sb.append("    }");
            }
        }

        sb.append("\n  }\n");
        sb.append("}\n");

        try {
            response.setContentType("text/plain");
            OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream());
            writer.write(sb.toString());
            writer.flush();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }
}
package voldemort.server.http.gui;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Date;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.server.ServiceType;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.server.socket.SocketService;
import voldemort.store.stats.Tracked;
import voldemort.store.Store;
import voldemort.utils.Utils;
import voldemort.utils.ByteArray;
import voldemort.VoldemortException;

import com.google.common.collect.Maps;

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
        }
        catch (UnknownHostException e) {
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

        String format =  request.getParameter("format");

        if ("json".equals(format)) {

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

            // general stats go here

            sb.append("\n  \"storestats\": {");

            for (Store<ByteArray, byte[]> store :  server.getStoreRepository().getAllLocalStores() ) {

                sb.append("\n    \"");
                sb.append(store.getName());
                sb.append("\" : {} \n");
            }

            sb.append("  }\n");
            sb.append("}\n");

            try {
                OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream());
                writer.write(sb.toString());
                writer.flush();
            }
            catch (Exception e) {
                throw new VoldemortException(e);
            }

            return;
        }

        Map<String, Object> params = Maps.newHashMap();
        params.put("status", socketService.getStatusManager());
        params.put("counters", Tracked.values());
        params.put("stores", server.getStoreRepository().getAllLocalStores());
        velocityEngine.render("status.vm", params, response.getOutputStream());

    }
}
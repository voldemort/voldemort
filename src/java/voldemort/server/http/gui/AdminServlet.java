package voldemort.server.http.gui;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

/**
 * Main servlet for the admin interface
 * 
 * @author jay
 * 
 */
public class AdminServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VoldemortServer server;
    private VelocityEngine velocityEngine;

    /* For use by servlet container */
    public AdminServlet() {}

    public AdminServlet(VoldemortServer server, VelocityEngine engine) {
        this.server = Objects.nonNull(server);
        this.velocityEngine = Objects.nonNull(engine);
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.server = (VoldemortServer) Objects.nonNull(getServletContext().getAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        Map<String, Object> params = Maps.newHashMap();
        params.put("cluster", server.getCluster());
        params.put("stores", server.getStoreMap());
        params.put("services", server.getServices());
        velocityEngine.render("admin.vm", params, response.getOutputStream());
    }

}

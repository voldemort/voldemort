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

package voldemort.server.http.gui;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

/**
 * Main servlet for the admin interface
 * 
 * 
 */
public class AdminServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VoldemortServer server;
    private VelocityEngine velocityEngine;

    /* For use by servlet container */
    public AdminServlet() {}

    public AdminServlet(VoldemortServer server, VelocityEngine engine) {
        this.server = Utils.notNull(server);
        this.velocityEngine = Utils.notNull(engine);
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
        Map<String, Object> params = Maps.newHashMap();
        params.put("cluster", server.getMetadataStore().getCluster());
        params.put("repository", server.getStoreRepository());
        params.put("services", server.getServices());
        velocityEngine.render("admin.vm", params, response.getOutputStream());
    }

}

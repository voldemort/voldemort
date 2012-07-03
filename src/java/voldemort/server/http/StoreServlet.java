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

package voldemort.server.http;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import voldemort.common.service.ServiceType;
import voldemort.server.VoldemortServer;
import voldemort.server.protocol.RequestHandler;
import voldemort.utils.Utils;

/**
 * Handles requests from HttpStores and multiplexes them to the appropriate
 * sub-store for actual storage
 * 
 * 
 */
public class StoreServlet extends HttpServlet {

    private static final Logger logger = Logger.getLogger(StoreServlet.class);
    private static final long serialVersionUID = 1;

    private RequestHandler requestHandler;

    /* For use by servlet container */
    public StoreServlet() {}

    public StoreServlet(RequestHandler handler) {
        this.requestHandler = handler;
    }

    @Override
    public void init() throws ServletException {
        super.init();
        // if we don't already have a stores map, attempt to initialize from the
        // servlet context
        if(this.requestHandler == null) {
            ServletContext context = this.getServletContext();
            VoldemortServer server = (VoldemortServer) Utils.notNull(context.getAttribute(VoldemortServletContextListener.SERVER_KEY));
            HttpService httpService = (HttpService) server.getService(ServiceType.HTTP);
            this.requestHandler = httpService.getRequestHandler();
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            requestHandler.handleRequest(new DataInputStream(request.getInputStream()),
                                         new DataOutputStream(response.getOutputStream()));

        } catch(Exception e) {
            logger.error("Uncaught exception in store servlet:", e);
            response.sendError(HttpURLConnection.HTTP_UNAVAILABLE, e.getMessage());
        }
    }

}

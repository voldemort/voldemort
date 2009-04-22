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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.server.storage.StorageService;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.RandomAccessFileStore;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

public class ReadOnlyStoreManagementServlet extends HttpServlet {

    private static final long serialVersionUID = 1;
    private static final Logger logger = Logger.getLogger(ReadOnlyStoreManagementServlet.class);

    private Map<String, RandomAccessFileStore> stores;
    private VelocityEngine velocityEngine;
    private FileFetcher fileFetcher;

    public ReadOnlyStoreManagementServlet(VoldemortServer server, VelocityEngine engine) {
        this.stores = getReadOnlyStores(server);
        this.velocityEngine = Utils.notNull(engine);
        String className = server.getVoldemortConfig()
                                 .getAllProps()
                                 .getString("file.fetcher.class", null);
        if(className == null) {
            this.fileFetcher = null;
        } else {
            try {
                this.fileFetcher = (FileFetcher) Class.forName(className).newInstance();
            } catch(Exception e) {
                throw new VoldemortException("Error loading file fetcher class " + className);
            }
        }
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.stores = getReadOnlyStores((VoldemortServer) getServletContext().getAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY));
        this.velocityEngine = (VelocityEngine) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY));
    }

    private Map<String, RandomAccessFileStore> getReadOnlyStores(VoldemortServer server) {
        StorageService storage = (StorageService) Utils.notNull(server)
                                                       .getService("storage-service");
        return storage.getReadOnlyStores();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        Map<String, Object> params = Maps.newHashMap();
        params.put("stores", stores);
        velocityEngine.render("read-only-mgmt.vm", params, resp.getOutputStream());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        try {
            if("swap".equals(getRequired(req, "operation"))) {
                String indexFile = getRequired(req, "index");
                String dataFile = getRequired(req, "data");
                String storeName = getRequired(req, "store");
                if(!stores.containsKey(storeName))
                    throw new ServletException("'" + storeName
                                               + "' is not a registered read-only store.");
                if(!Utils.isReadableFile(indexFile))
                    throw new ServletException("Index file '" + indexFile
                                               + "' is not a readable file.");
                if(!Utils.isReadableFile(dataFile))
                    throw new ServletException("Data file '" + dataFile
                                               + "' is not a readable file.");

                RandomAccessFileStore store = stores.get(storeName);
                store.swapFiles(indexFile, dataFile);
                resp.getWriter().write("Swap completed.");
            } else if("fetch".equals(getRequired(req, "operation"))) {
                String indexUrl = getRequired(req, "index");
                String dataUrl = getRequired(req, "data");

                // fetch the files if necessary
                File indexFile;
                File dataFile;
                if(fileFetcher == null) {
                    indexFile = new File(indexUrl);
                    dataFile = new File(dataUrl);
                } else {
                    logger.info("Executing fetch of " + indexUrl);
                    indexFile = fileFetcher.fetchFile(indexUrl);
                    logger.info("Executing fetch of " + dataUrl);
                    dataFile = fileFetcher.fetchFile(dataUrl);
                    logger.info("Fetch complete.");
                }
                resp.getWriter().write(indexFile.getAbsolutePath());
                resp.getWriter().write("\n");
                resp.getWriter().write(dataFile.getAbsolutePath());
            } else {
                throw new IllegalArgumentException("Unknown operation parameter: "
                                                   + req.getParameter("operation"));
            }
        } catch(Exception e) {
            resp.sendError(500, "Error while performing operation: " + e.getMessage());
        }
    }

    private String getRequired(HttpServletRequest req, String name) throws ServletException {
        String val = req.getParameter(name);
        if(val == null)
            throw new ServletException("Missing required parameter '" + name + "'.");
        return val;
    }
}

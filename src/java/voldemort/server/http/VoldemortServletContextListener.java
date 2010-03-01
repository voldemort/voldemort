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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.http.gui.VelocityEngine;
import voldemort.utils.ConfigurationException;

/**
 * A helper class that loads the Voldemort server by bootstrapping from the
 * VOLDEMORT_HOME environment variable.
 * 
 * 
 */
public class VoldemortServletContextListener implements ServletContextListener {

    public static final String VOLDEMORT_TEMPLATE_DIR = "voldemort/server/http/gui/templates";
    public static final String SERVER_KEY = "vldmt_server";
    public static final String SERVER_CONFIG_KEY = "vldmt_config";
    public static final String VELOCITY_ENGINE_KEY = "vldmt_velocity_engine";

    private static final Logger logger = Logger.getLogger(VoldemortServletContextListener.class.getName());

    public void contextDestroyed(ServletContextEvent event) {
        logger.info("Calling application shutdown...");
        VoldemortServer server = (VoldemortServer) event.getServletContext()
                                                        .getAttribute(SERVER_KEY);
        if(server != null)
            server.stop();
        logger.info("Destroying application...");
        event.getServletContext().removeAttribute(SERVER_KEY);
        event.getServletContext().removeAttribute(SERVER_CONFIG_KEY);
        event.getServletContext().removeAttribute(VELOCITY_ENGINE_KEY);
    }

    public void contextInitialized(ServletContextEvent event) {
        try {
            logger.info("Creating application...");
            VoldemortServer server = new VoldemortServer(VoldemortConfig.loadFromEnvironmentVariable());
            event.getServletContext().setAttribute(SERVER_KEY, server);
            event.getServletContext().setAttribute(SERVER_CONFIG_KEY, server.getVoldemortConfig());
            event.getServletContext().setAttribute(VELOCITY_ENGINE_KEY,
                                                   new VelocityEngine(VOLDEMORT_TEMPLATE_DIR));
            server.start();
            logger.info("Application created.");
        } catch(ConfigurationException e) {
            logger.info("Error loading voldemort server:", e);
            throw e;
        } catch(Exception e) {
            logger.error("Error loading voldemort server:", e);
            throw new ConfigurationException(e);
        }
    }

}

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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import voldemort.VoldemortException;
import voldemort.utils.Utils;

/**
 * Wrapper class that abstracts some of the configuration details and wraps some
 * irritating exceptions.
 * 
 * 
 */
public class VelocityEngine {

    private final String baseTemplateDir;
    private final org.apache.velocity.app.VelocityEngine engine;

    public VelocityEngine(String baseTemplateDir) {
        this.baseTemplateDir = Utils.notNull(baseTemplateDir);
        this.engine = new org.apache.velocity.app.VelocityEngine();
        engine.setProperty("resource.loader", "classpath");
        engine.setProperty("classpath.resource.loader.class",
                           ClasspathResourceLoader.class.getName());
        engine.setProperty("classpath.resource.loader.cache", false);
        engine.setProperty("file.resource.loader.modificationCheckInterval", 0);
        engine.setProperty("input.encoding", "UTF-8");
        engine.setProperty("velocimacro.permissions.allow.inline", true);
        engine.setProperty("velocimacro.library.autoreload", true);
        engine.setProperty("runtime.log.logsystem.class", Log4JLogChute.class);
        engine.setProperty("runtime.log.logsystem.log4j.logger",
                           Logger.getLogger("org.apache.velocity.Logger"));
        engine.setProperty("parser.pool.size", 3);
    }

    public void render(String template, Map<String, Object> map, OutputStream stream) {
        VelocityContext context = new VelocityContext(map);
        OutputStreamWriter writer = new OutputStreamWriter(stream);
        try {
            engine.mergeTemplate(baseTemplateDir + "/" + template, "UTF-8", context, writer);
            writer.flush();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

}

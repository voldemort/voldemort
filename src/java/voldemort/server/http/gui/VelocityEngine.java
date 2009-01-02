package voldemort.server.http.gui;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import voldemort.VoldemortException;

import com.google.common.base.Objects;

/**
 * Wrapper class that abstracts some of the configuration details and wraps some
 * irritating exceptions.
 * 
 * @author jay
 * 
 */
public class VelocityEngine {

    private final String baseTemplateDir;
    private final org.apache.velocity.app.VelocityEngine engine;

    public VelocityEngine(String baseTemplateDir) {
        this.baseTemplateDir = Objects.nonNull(baseTemplateDir);
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
        } catch (Exception e) {
            throw new VoldemortException(e);
        }
    }

}

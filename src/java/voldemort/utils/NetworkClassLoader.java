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

package voldemort.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Network class Loader to load Classes on different JVMs needed to provide a
 * User Defined Functions (Filter API) on the server side w/o needing to add jar
 * on each server.
 * 
 * 
 */
public class NetworkClassLoader extends ClassLoader {

    private final static Logger logger = Logger.getLogger(NetworkClassLoader.class);

    public NetworkClassLoader(ClassLoader parentClassLoader) {
        super(parentClassLoader);
    }

    public Class<?> loadClass(String className, byte[] classBuffer, int offset, int length) {
        Class<?> loadedClass = super.findLoadedClass(className);

        if(null == loadedClass) {
            return super.defineClass(className, classBuffer, offset, length);
        }

        return loadedClass;
    }

    /**
     * Utility function to convert Class --> byte[] <br>
     * call {@link ClassLoader#getResource(String)} internally to find the class
     * file and then dump the bytes[]
     * 
     * @param cl The class
     * @return Byte representation of the class
     * @throws IOException
     */
    public byte[] dumpClass(Class<?> cl) throws IOException {
        // get class fileName
        String filename = cl.getName().replace('.', File.separatorChar) + ".class";
        InputStream in = null;
        logger.debug("NetworkClassloader dumpClass() :" + cl.getCanonicalName());
        try {
            in = this.getResourceAsStream(filename);
            byte[] classBytes = IOUtils.toByteArray(in);
            return classBytes;
        } finally {
            if(null != in)
                in.close();
        }
    }
}

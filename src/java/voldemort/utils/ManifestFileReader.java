/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.log4j.Logger;

/**
 * A utility class that abstract out fields from manifest file
 * 
 */
public class ManifestFileReader {

    protected static final Logger logger = Logger.getLogger(ManifestFileReader.class);

    private static String MANIFEST_FILE = "META-INF/MANIFEST.MF";

    private static String RELEASE_VERSION_KEY = "Voldemort-Implementation-Version";

    public static String getReleaseVersion() {

        try {
            Enumeration<URL> resources = ManifestFileReader.class.getClassLoader()
                                                                 .getResources(MANIFEST_FILE);
            while(resources.hasMoreElements()) {

                Manifest manifest = new Manifest(resources.nextElement().openStream());

                Attributes mainAttribs = manifest.getMainAttributes();
                String version = mainAttribs.getValue(RELEASE_VERSION_KEY);

                if(version != null) {
                    logger.debug("Voldemort Release version is:" + version);
                    return version;
                }

            }
        } catch(IOException IoE) {
            logger.warn("Unable to load voldemort release version, could not find a manifest file");
        }

        return null;

    }
}

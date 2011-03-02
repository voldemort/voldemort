/*
 * Copyright 2011 LinkedIn, Inc
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

package voldemort.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SerializationUtils {

    private static final Logger logger = Logger.getLogger(SerializationUtils.class);

    /**
     * Extracts the java class name from the schema info
     * 
     * @param schemaInfo the schema info, a string like: java=java.lang.String
     * @return the name of the class extracted from the schema info
     */
    public static String getJavaClassFromSchemaInfo(String schemaInfo) {
        final String ONLY_JAVA_CLIENTS_SUPPORTED = "Only Java clients are supported currently, so the format of the schema-info should be: <schema-info>java=foo.Bar</schema-info> where foo.Bar is the fully qualified name of the message.";

        if(StringUtils.isEmpty(schemaInfo))
            throw new IllegalArgumentException("This serializer requires a non-empty schema-info.");

        String[] languagePairs = StringUtils.split(schemaInfo, ',');
        if(languagePairs.length > 1)
            throw new IllegalArgumentException(ONLY_JAVA_CLIENTS_SUPPORTED);

        String[] javaPair = StringUtils.split(languagePairs[0], '=');
        if(javaPair.length != 2 || !javaPair[0].trim().equals("java"))
            throw new IllegalArgumentException(ONLY_JAVA_CLIENTS_SUPPORTED);

        return javaPair[1].trim();
    }

    public static void close(ByteArrayOutputStream stream) {
        if(stream != null) {
            try {
                stream.close();
            } catch(IOException e) {
                logger.error("Failed to close stream", e);
            }
        }
    }

}

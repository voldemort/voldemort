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

package voldemort.serialization.json;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Writes JSON Objects as String recognized by {@link JsonReader}
 * 
 * 
 */
public class JsonWriter {

    private final Writer writer;

    public JsonWriter(Writer writer) {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    public void write(Object o) {

        try {
            if(null == o) {
                writer.write("null");
            } else if(o instanceof Map) {
                writeMap((Map<String, Object>) o);
            } else if(o instanceof List) {
                writeList((List<Object>) o);
            } else if(o instanceof Date) {
                writeDate((Date) o);
            } else if(o instanceof String) {
                writer.write('"');
                writer.write((String) o);
                writer.write('"');
            } else {
                writer.write(o.toString());
            }
        } catch(Exception e) {
            throw new RuntimeException("JsonWriter failed to write Object(" + o + ")  as String", e);
        }
    }

    public void writeMap(Map<String, Object> values) throws IOException {
        writer.write('{');
        Set<Map.Entry<String, Object>> entrySet = values.entrySet();
        int index = 0;
        for(Map.Entry<String, Object> entry: entrySet) {
            writer.write('"');
            writer.write(entry.getKey());
            writer.write('"');

            writer.write(':');

            // write Object
            write(entry.getValue());

            if(++index < entrySet.size()) {
                writer.write(", ");
            }
        }
        writer.write('}');
    }

    public void writeList(List<Object> list) throws IOException {
        writer.write('[');
        int index = 0;
        for(Object entry: list) {
            write(entry);

            if(++index < list.size()) {
                writer.write(", ");

            }
        }
        writer.write(']');
    }

    public void writeDate(Date date) throws IOException {
        writer.write("" + date.getTime());
    }
}

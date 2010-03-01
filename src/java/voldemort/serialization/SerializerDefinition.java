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

package voldemort.serialization;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * A SerializerDefinition holds all the meta information for a serializer.
 * 
 * 
 */
public class SerializerDefinition {

    private final String name;
    private final Integer currentSchemaVersion;
    private final boolean hasVersion;
    private final Map<Integer, String> schemaInfoByVersion;
    private final Compression compression;

    public SerializerDefinition(String name) {
        super();
        this.name = Utils.notNull(name);
        this.currentSchemaVersion = -1;
        this.schemaInfoByVersion = new HashMap<Integer, String>();
        compression = null;
        this.hasVersion = true;
    }

    public SerializerDefinition(String name, String schemaInfo) {
        super();
        this.name = Utils.notNull(name);
        this.currentSchemaVersion = 0;
        this.schemaInfoByVersion = new HashMap<Integer, String>();
        this.schemaInfoByVersion.put(0, schemaInfo);
        compression = null;
        this.hasVersion = true;
    }

    public SerializerDefinition(String name,
                                Map<Integer, String> schemaInfos,
                                boolean hasVersion,
                                Compression compression) {
        super();
        this.name = Utils.notNull(name);
        this.schemaInfoByVersion = new HashMap<Integer, String>();
        this.compression = compression;
        this.hasVersion = hasVersion;
        if(!hasVersion) {
            this.currentSchemaVersion = 0;
            if(schemaInfos.size() != 1)
                throw new IllegalArgumentException("Schema version = none, but multiple schemas specified.");
            String schema = schemaInfos.values().iterator().next();
            this.schemaInfoByVersion.put(0, schema);
        } else {
            int max = -1;
            for(Integer key: schemaInfos.keySet()) {
                if(key < 0)
                    throw new IllegalArgumentException("Version cannot be less than 0.");
                else if(key > Byte.MAX_VALUE)
                    throw new IllegalArgumentException("Version cannot be more than "
                                                       + Byte.MAX_VALUE);
                if(key > max)
                    max = key;
                this.schemaInfoByVersion.put(key, schemaInfos.get(key));
            }
            this.currentSchemaVersion = max;
        }
    }

    public String getName() {
        return name;
    }

    public int getCurrentSchemaVersion() {
        if(currentSchemaVersion < 0)
            throw new IllegalStateException("There is no schema info associated with this serializer definition.");
        return currentSchemaVersion;
    }

    public Map<Integer, String> getAllSchemaInfoVersions() {
        return this.schemaInfoByVersion;
    }

    public boolean hasSchemaInfo() {
        return this.currentSchemaVersion >= 0;
    }

    public String getSchemaInfo(int version) {
        if(!schemaInfoByVersion.containsKey(version))
            throw new IllegalArgumentException("Unknown schema version " + version + ".");
        return schemaInfoByVersion.get(version);
    }

    public String getCurrentSchemaInfo() {
        if(currentSchemaVersion < 0)
            throw new IllegalStateException("There is no schema info associated with this serializer definition.");
        return schemaInfoByVersion.get(this.currentSchemaVersion);
    }

    public boolean hasVersion() {
        return this.hasVersion;
    }

    public boolean hasCompression() {
        return compression != null;
    }

    public Compression getCompression() {
        return compression;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;
        if(obj == null)
            return false;
        if(!(obj.getClass() == SerializerDefinition.class))
            return false;
        SerializerDefinition s = (SerializerDefinition) obj;
        return Objects.equal(getName(), s.getName())
               && Objects.equal(this.schemaInfoByVersion, s.schemaInfoByVersion)
               && Objects.equal(this.compression, s.compression)
               && this.hasVersion == s.hasVersion();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] { name, this.schemaInfoByVersion, compression,
                hasVersion });
    }

    @Override
    public String toString() {
        return "SerializerDefinition(name = " + this.name + ", schema-info = "
               + this.schemaInfoByVersion + ", compression = " + compression + ")";
    }
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.utils.ByteUtils;

/**
 * A serializer that goes from a simple JSON like object definition + an object
 * instance to serialized bytes and back again.
 * 
 * Official motto of this class: "I fought the static type system, and the type
 * system won."
 * 
 * 
 */
public class JsonTypeSerializer implements Serializer<Object> {

    private static final int MAX_SEQ_LENGTH = 0x3FFFFFFF;

    private final boolean hasVersion;
    private final SortedMap<Integer, JsonTypeDefinition> typeDefVersions;

    public JsonTypeSerializer(String typeDef) {
        this(JsonTypeDefinition.fromJson(typeDef));
    }

    public JsonTypeSerializer(String typeDef, boolean hasVersion) {
        this(JsonTypeDefinition.fromJson(typeDef), hasVersion);
    }

    public JsonTypeSerializer(JsonTypeDefinition typeDef) {
        this.hasVersion = false;
        this.typeDefVersions = new TreeMap<Integer, JsonTypeDefinition>();
        this.typeDefVersions.put(0, typeDef);
    }

    public JsonTypeSerializer(JsonTypeDefinition typeDef, boolean hasVersion) {
        this.hasVersion = hasVersion;
        this.typeDefVersions = new TreeMap<Integer, JsonTypeDefinition>();
        this.typeDefVersions.put(0, typeDef);
    }

    public JsonTypeSerializer(Map<Integer, JsonTypeDefinition> typeDefVersions) {
        this.hasVersion = true;
        this.typeDefVersions = new TreeMap<Integer, JsonTypeDefinition>(typeDefVersions);
    }

    public byte[] toBytes(Object object) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        try {
            toBytes(object, output);
            output.flush();
            return bytes.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public void toBytes(Object object, DataOutputStream output) throws IOException {
        Integer newestVersion = typeDefVersions.lastKey();
        JsonTypeDefinition typeDef = typeDefVersions.get(newestVersion);
        if(hasVersion)
            output.writeByte(newestVersion.byteValue());
        write(output, object, typeDef.getType());
    }

    public Object toObject(byte[] bytes) {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            return toObject(input);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public Object toObject(DataInputStream input) throws IOException {
        Integer version = 0;
        if(hasVersion)
            version = Integer.valueOf(input.readByte());
        JsonTypeDefinition typeDef = typeDefVersions.get(version);
        if(typeDef == null)
            throw new SerializationException("No schema found for schema version " + version + ".");
        return read(input, typeDef.getType());
    }

    @SuppressWarnings("unchecked")
    private void write(DataOutputStream output, Object object, Object type) throws IOException {
        try {
            if(type instanceof Map) {
                if(object != null && !(object instanceof Map))
                    throw new SerializationException("Expected Map, but got " + object.getClass()
                                                     + ": " + object);
                writeMap(output, (Map<String, Object>) object, (Map<String, Object>) type);
            } else if(type instanceof List) {
                if(object != null && !(object instanceof List))
                    throw new SerializationException("Expected List but got " + object.getClass()
                                                     + ": " + object);
                writeList(output, (List<Object>) object, (List<Object>) type);
            } else if(type instanceof JsonTypes) {
                JsonTypes jsonType = (JsonTypes) type;
                switch(jsonType) {
                    case STRING:
                        writeString(output, (String) object);
                        break;
                    case INT8:
                        writeInt8(output, (Byte) object);
                        break;
                    case INT16:
                        writeInt16(output, coerceToShort(object));
                        break;
                    case INT32:
                        writeInt32(output, coerceToInteger(object));
                        break;
                    case INT64:
                        writeInt64(output, coerceToLong(object));
                        break;
                    case FLOAT32:
                        writeFloat32(output, coerceToFloat(object));
                        break;
                    case FLOAT64:
                        writeFloat64(output, coerceToDouble(object));
                        break;
                    case DATE:
                        writeDate(output, coerceToDate(object));
                        break;
                    case BYTES:
                        writeBytes(output, (byte[]) object);
                        break;
                    case BOOLEAN:
                        writeBoolean(output, (Boolean) object);
                        break;
                    default:
                        throw new SerializationException("Unknown type: " + type);
                }
            }
        } catch(ClassCastException e) {
            // simpler than doing every test
            throw new SerializationException("Expected type " + type
                                             + " but got object of incompatible type "
                                             + object.getClass().getName() + ".", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Object read(DataInputStream stream, Object type) throws IOException {
        if(type instanceof Map) {
            return readMap(stream, (Map<String, Object>) type);
        } else if(type instanceof List) {
            return readList(stream, (List<?>) type);
        } else if(type instanceof JsonTypes) {
            JsonTypes jsonType = (JsonTypes) type;
            switch(jsonType) {
                case BOOLEAN:
                    return readBoolean(stream);
                case INT8:
                    return readInt8(stream);
                case INT16:
                    return readInt16(stream);
                case INT32:
                    return readInt32(stream);
                case INT64:
                    return readInt64(stream);
                case FLOAT32:
                    return readFloat32(stream);
                case FLOAT64:
                    return readFloat64(stream);
                case DATE:
                    return readDate(stream);
                case BYTES:
                    return readBytes(stream);
                case STRING:
                    return readString(stream);
                default:
                    throw new SerializationException("Unknown type: " + type);
            }
        } else {
            throw new SerializationException("Unknown type of class " + type.getClass());
        }
    }

    private void writeBoolean(DataOutputStream output, Boolean b) throws IOException {
        if(b == null)
            output.writeByte(-1);
        else if(b.booleanValue())
            output.writeByte(1);
        else
            output.write(0);
    }

    private Boolean readBoolean(DataInputStream stream) throws IOException {
        byte b = stream.readByte();
        if(b < 0)
            return null;
        else if(b == 0)
            return false;
        else
            return true;
    }

    private Short coerceToShort(Object o) {
        if(o == null)
            return null;
        Class<?> c = o.getClass();
        if(c == Short.class)
            return (Short) o;
        else if(c == Byte.class)
            return ((Byte) o).shortValue();
        else
            throw new SerializationException("Object of type " + c.getName()
                                             + " cannot be coerced to type " + JsonTypes.INT16
                                             + " as the schema specifies.");
    }

    private Integer coerceToInteger(Object o) {
        if(o == null)
            return null;
        Class<?> c = o.getClass();
        if(c == Integer.class)
            return (Integer) o;
        else if(c == Byte.class)
            return ((Byte) o).intValue();
        else if(c == Short.class)
            return ((Short) o).intValue();
        else
            throw new SerializationException("Object of type " + c.getName()
                                             + " cannot be coerced to type " + JsonTypes.INT32
                                             + " as the schema specifies.");
    }

    private Long coerceToLong(Object o) {
        if(o == null)
            return null;
        Class<?> c = o.getClass();
        if(c == Long.class)
            return (Long) o;
        else if(c == Byte.class)
            return ((Byte) o).longValue();
        else if(c == Short.class)
            return ((Short) o).longValue();
        else if(c == Integer.class)
            return ((Integer) o).longValue();
        else
            throw new SerializationException("Object of type " + c.getName()
                                             + " cannot be coerced to type " + JsonTypes.INT64
                                             + " as the schema specifies.");
    }

    private Float coerceToFloat(Object o) {
        if(o == null)
            return null;
        Class<?> c = o.getClass();
        if(c == Float.class)
            return (Float) o;
        else if(c == Byte.class)
            return ((Byte) o).floatValue();
        else if(c == Short.class)
            return ((Short) o).floatValue();
        else if(c == Integer.class)
            return ((Integer) o).floatValue();
        else
            throw new SerializationException("Object of type " + c.getName()
                                             + " cannot be coerced to type " + JsonTypes.FLOAT32
                                             + " as the schema specifies.");
    }

    private Double coerceToDouble(Object o) {
        if(o == null)
            return null;
        Class<?> c = o.getClass();
        if(c == Double.class)
            return (Double) o;
        else if(c == Byte.class)
            return ((Byte) o).doubleValue();
        else if(c == Short.class)
            return ((Short) o).doubleValue();
        else if(c == Integer.class)
            return ((Integer) o).doubleValue();
        else if(c == Float.class)
            return ((Float) o).doubleValue();
        else
            throw new SerializationException("Object of type " + c.getName()
                                             + " cannot be coerced to type " + JsonTypes.FLOAT32
                                             + " as the schema specifies.");
    }

    private void writeString(DataOutputStream stream, String s) throws IOException {
        writeBytes(stream, s == null ? null : s.getBytes("UTF-8"));
    }

    private String readString(DataInputStream stream) throws IOException {
        byte[] bytes = readBytes(stream);
        if(bytes == null)
            return null;
        else
            return new String(bytes, "UTF-8");
    }

    private Byte readInt8(DataInputStream stream) throws IOException {
        byte b = stream.readByte();
        if(b == Byte.MIN_VALUE)
            return null;
        else
            return b;
    }

    private void writeInt8(DataOutputStream output, Byte b) throws IOException {
        if(b == null)
            output.writeByte(Byte.MIN_VALUE);
        else if(b.byteValue() == Byte.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store " + Byte.MIN_VALUE
                                             + " in int8, but minimum value is "
                                             + (Byte.MIN_VALUE - 1) + ".");
        else
            output.writeByte(b.byteValue());
    }

    private Short readInt16(DataInputStream stream) throws IOException {
        short s = stream.readShort();
        if(s == Short.MIN_VALUE)
            return null;
        else
            return s;
    }

    private void writeInt16(DataOutputStream output, Short s) throws IOException {
        if(s == null)
            output.writeShort(Short.MIN_VALUE);
        else if(s.shortValue() == Short.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store " + Short.MIN_VALUE
                                             + " in int16, but minimum value is "
                                             + (Short.MIN_VALUE - 1) + ".");
        else
            output.writeShort(s.shortValue());
    }

    private Integer readInt32(DataInputStream stream) throws IOException {
        int i = stream.readInt();
        if(i == Integer.MIN_VALUE)
            return null;
        else
            return i;
    }

    private void writeInt32(DataOutputStream output, Integer i) throws IOException {
        if(i == null)
            output.writeInt(Integer.MIN_VALUE);
        else if(i.intValue() == Integer.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store " + Integer.MIN_VALUE
                                             + " in int32, but minimum value is "
                                             + (Integer.MIN_VALUE - 1) + ".");
        else
            output.writeInt(i.intValue());
    }

    private Long readInt64(DataInputStream stream) throws IOException {
        long l = stream.readLong();
        if(l == Long.MIN_VALUE)
            return null;
        else
            return l;
    }

    private void writeInt64(DataOutputStream output, Long l) throws IOException {
        if(l == null)
            output.writeLong(Long.MIN_VALUE);
        else if(l.longValue() == Long.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store " + Long.MIN_VALUE
                                             + " in int64, but minimum value is "
                                             + (Long.MIN_VALUE - 1) + ".");
        else
            output.writeLong(l.longValue());
    }

    private Float readFloat32(DataInputStream stream) throws IOException {
        float f = stream.readFloat();
        if(f == Float.MIN_VALUE)
            return null;
        else
            return f;
    }

    private void writeFloat32(DataOutputStream output, Float f) throws IOException {
        if(f == null)
            output.writeFloat(Float.MIN_VALUE);
        else if(f.floatValue() == Float.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store " + Float.MIN_VALUE
                                             + " in float32, but that value is reserved for null.");
        else
            output.writeFloat(f.floatValue());
    }

    private Double readFloat64(DataInputStream stream) throws IOException {
        double d = stream.readDouble();
        if(d == Double.MIN_VALUE)
            return null;
        else
            return d;
    }

    private void writeFloat64(DataOutputStream output, Double d) throws IOException {
        if(d == null)
            output.writeDouble(Double.MIN_VALUE);
        else if(d.doubleValue() == Double.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store " + Double.MIN_VALUE
                                             + " in float64, but that value is reserved for null.");
        else
            output.writeDouble(d.doubleValue());
    }

    private Date coerceToDate(Object o) {
        if(o == null)
            return null;
        else if(o instanceof Date)
            return (Date) o;
        else if(o instanceof Number)
            return new Date(((Number) o).longValue());
        else
            throw new SerializationException("Object of type " + o.getClass()
                                             + " can not be coerced to type " + JsonTypes.DATE);
    }

    private Date readDate(DataInputStream stream) throws IOException {
        long l = stream.readLong();
        if(l == Long.MIN_VALUE)
            return null;
        else
            return new Date(l);
    }

    private void writeDate(DataOutputStream output, Date d) throws IOException {
        if(d == null)
            output.writeLong(Long.MIN_VALUE);
        else if(d.getTime() == Long.MIN_VALUE)
            throw new SerializationException("Underflow: attempt to store "
                                             + new Date(Long.MIN_VALUE)
                                             + " in date, but that value is reserved for null.");
        else
            output.writeLong(d.getTime());
    }

    private byte[] readBytes(DataInputStream stream) throws IOException {
        int size = readLength(stream);
        if(size < 0)
            return null;
        byte[] bytes = new byte[size];
        ByteUtils.read(stream, bytes);
        return bytes;
    }

    private void writeBytes(DataOutputStream output, byte[] b) throws IOException {
        if(b == null) {
            writeLength(output, -1);
        } else {
            writeLength(output, b.length);
            output.write(b);
        }
    }

    private void writeMap(DataOutputStream output,
                          Map<String, Object> object,
                          Map<String, Object> type) throws IOException {
        if(object == null) {
            output.writeByte(-1);
            return;
        } else {
            output.writeByte(1);
            if(object.size() != type.size())
                throw new SerializationException("Invalid map for serialization, expected: " + type
                                                 + " but got " + object);
            for(Map.Entry<String, Object> entry: type.entrySet()) {
                if(!object.containsKey(entry.getKey()))
                    throw new SerializationException("Missing property: " + entry.getKey()
                                                     + " that is required by the type (" + type
                                                     + ")");
                try {
                    write(output, object.get(entry.getKey()), entry.getValue());
                } catch(SerializationException e) {
                    throw new SerializationException("Fail to write property: " + entry.getKey(), e);
                }
            }
        }
    }

    private Map<?, ?> readMap(DataInputStream stream, Map<String, Object> type) throws IOException {
        if(stream.readByte() == -1)
            return null;
        Map<String, Object> m = new HashMap<String, Object>(type.size());
        for(Map.Entry<String, Object> typeMapEntry: type.entrySet())
            m.put(typeMapEntry.getKey(), read(stream, typeMapEntry.getValue()));
        return m;
    }

    private void writeList(DataOutputStream output, List<Object> objects, List<Object> type)
            throws IOException {
        if(type.size() != 1)
            throw new SerializationException("Invalid type: expected single value type in list: "
                                             + type);
        Object entryType = type.get(0);
        if(objects == null) {
            writeLength(output, -1);
        } else {
            writeLength(output, objects.size());
            for(Object o: objects)
                write(output, o, entryType);
        }
    }

    private List<?> readList(DataInputStream stream, List<?> type) throws IOException {
        int size = readLength(stream);
        if(size < 0)
            return null;
        List<Object> items = new ArrayList<Object>(size);
        Object entryType = type.get(0);
        for(int i = 0; i < size; i++)
            items.add(read(stream, entryType));
        return items;
    }

    private void writeLength(DataOutputStream stream, int size) throws IOException {
        if(size < Short.MAX_VALUE) {
            stream.writeShort(size);
        } else if(size <= MAX_SEQ_LENGTH) {
            stream.writeInt(size | 0xC0000000);
        } else {
            throw new SerializationException("Invalid length: maximum is " + MAX_SEQ_LENGTH);
        }
    }

    int readLength(DataInputStream stream) throws IOException {
        short size = stream.readShort();
        // this is a hack for backwards compatibility
        if(size == -1) {
            return -1;
        } else if(size < -1) {
            // mask off first two bits, remainder is the size
            int fixedSize = size & 0x3FFF;
            fixedSize <<= 16;
            fixedSize += stream.readShort() & 0xFFFF;
            return fixedSize;
        } else {
            return size;
        }
    }
}

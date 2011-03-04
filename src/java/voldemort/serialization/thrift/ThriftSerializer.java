package voldemort.serialization.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;

/**
 * ThriftSerializer uses one of the Thrift protocols (binary, json and
 * simple-json) to serialize and deserialize a Thrift generated object.
 * <p>
 * 
 * The following is a sample configuration for value-serializer:
 * 
 * <pre>
 * &lt;value-serializer&gt;
 *   &lt;type&gt;thrift&lt;/type&gt;
 *   &lt;schema-info&gt;java=com.linkedin.foobar.FooMessage,protocol=binary&lt;/schema-info&gt;
 * &lt;/value-serializer&gt;
 * </pre>
 * 
 * Currently, only Java clients are supported. Once support for clients in other
 * languages is available, a semi-colon separated list will be accepted for the
 * schema-info element (one for each language).
 */
public class ThriftSerializer<T extends TBase<?, ?>> implements Serializer<T> {

    private static final String ONLY_JAVA_CLIENTS_SUPPORTED = "Only Java clients are supported currently, so the format of the schema-info should be: <schema-info>java=com.xyz.Foo,protocol=binary</schema-info> where com.xyz.Foo is the fully qualified name of the message.";

    /**
     * Supported Thrift Protocols
     */
    static enum ThriftProtocol {
        BINARY,
        JSON,
        SIMPLE_JSON,
        UNKNOWN
    }

    private Class<T> messageClass;
    private ThriftProtocol protocol;

    @SuppressWarnings("unchecked")
    public ThriftSerializer(String schemaInfo) {
        String[] thriftInfo = parseSchemaInfo(schemaInfo);

        if(thriftInfo[1] == null || thriftInfo[1].length() == 0) {
            throw new IllegalArgumentException("Thrift protocol is missing from schema-info.");
        }
        this.protocol = getThriftProtocol(thriftInfo[1]);
        if(this.protocol == ThriftProtocol.UNKNOWN) {
            throw new IllegalArgumentException("Unknown Thrift protocol found in schema-info");
        }

        if(thriftInfo[0] == null || thriftInfo[0].length() == 0) {
            throw new IllegalArgumentException("Thrift generated class name is missing from schema-info.");
        }
        try {
            this.messageClass = (Class<T>) Class.forName(thriftInfo[0]);
            Object msgObj = messageClass.newInstance();
            if(!(msgObj instanceof TBase)) {
                throw new IllegalArgumentException(thriftInfo[0]
                                                   + " is not a subtype of com.facebook.thrift.TBase");
            }
        } catch(ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        } catch(SecurityException e) {
            throw new IllegalArgumentException(e);
        } catch(InstantiationException e) {
            throw new IllegalArgumentException(e);
        } catch(IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] toBytes(T object) {
        MemoryBuffer buffer = new MemoryBuffer();
        TProtocol protocol = createThriftProtocol(buffer);
        try {
            object.write(protocol);
        } catch(TException e) {
            throw new SerializationException(e);
        }
        return buffer.toByteArray();
    }

    public T toObject(byte[] bytes) {
        MemoryBuffer buffer = new MemoryBuffer();
        try {
            buffer.write(bytes);
        } catch(TTransportException e) {
            throw new SerializationException(e);
        }
        TProtocol protocol = createThriftProtocol(buffer);

        T msg = null;
        try {
            msg = messageClass.newInstance();
            msg.read(protocol);
        } catch(InstantiationException e) {
            throw new SerializationException(e);
        } catch(IllegalAccessException e) {
            throw new SerializationException(e);
        } catch(TException e) {
            throw new SerializationException(e);
        }

        return msg;
    }

    protected String[] parseSchemaInfo(String schemaInfo) {
        String[] thriftInfo = new String[2];

        String javaToken = null;
        String[] tokens = schemaInfo.split(";");
        for(int i = 0; i < tokens.length; i++) {
            if(tokens[i].trim().startsWith("java")) {
                javaToken = tokens[i];
                break;
            }
        }

        if(javaToken == null) {
            throw new IllegalArgumentException(ONLY_JAVA_CLIENTS_SUPPORTED);
        }

        tokens = javaToken.split(",");
        for(int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
            if(tokens[i].startsWith("java=")) {
                thriftInfo[0] = tokens[i].substring("java=".length());
            } else if(tokens[i].startsWith("protocol=")) {
                thriftInfo[1] = tokens[i].substring("protocol=".length());
            }
        }

        return thriftInfo;
    }

    protected ThriftProtocol getThriftProtocol(String protocolStr) {
        if(protocolStr.equalsIgnoreCase("binary")) {
            return ThriftProtocol.BINARY;
        } else if(protocolStr.equalsIgnoreCase("json")) {
            return ThriftProtocol.JSON;
        } else if(protocolStr.equalsIgnoreCase("simple-json")) {
            return ThriftProtocol.SIMPLE_JSON;
        } else {
            return ThriftProtocol.UNKNOWN;
        }
    }

    protected TProtocol createThriftProtocol(TTransport transport) {
        switch(this.protocol) {
            case BINARY:
                return new TBinaryProtocol(transport);
            case JSON:
                return new TJSONProtocol(transport);
            case SIMPLE_JSON:
                return new TSimpleJSONProtocol(transport);
            default:
                throw new IllegalArgumentException("Unknown Thrift Protocol.");
        }
    }
}

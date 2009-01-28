package voldemort.serialization.protobuf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;

import voldemort.serialization.Serializer;

import com.google.protobuf.Message;

/**
 * A serializer that relies on Protocol Buffers for serialization and
 * deserialization. Only Java clients are supported currently, but there are
 * plans to support clients in other languages.
 * 
 * An example configuration of a value-serializer follows.
 * 
 * <pre>
 * &lt;value-serializer&gt;
 *   &lt;type&gt;protobuf&lt;/type&gt;
 *   &lt;schema-info&gt;java=com.linkedin.foobar.FooMessage&lt;/schema-info&gt;
 * &lt;/value-serializer&gt;
 * </pre>
 * 
 * Once support for clients in other languages is available, a comma-separated
 * list will be accepted for the schema-info element (one for each language).
 */
public class ProtoBufSerializer<T extends Message> implements Serializer<T> {

    private static final String ONLY_JAVA_CLIENTS_SUPPORTED = "Only Java clients are supported currently, so the format of the schema-info should be: <schema-info>java=foo.Bar</schema-info> where foo.Bar is the fully qualified name of the message.";

    private final Method parseFromMethod;
    private final Class<T> messageClass;

    @SuppressWarnings("unchecked")
    public ProtoBufSerializer(String currentSchemaInfo) {
        try {
            this.messageClass = (Class<T>) Class.forName(getJavaFqnForMessage(currentSchemaInfo),
                                                         false,
                                                         Thread.currentThread()
                                                               .getContextClassLoader());

            if(!Message.class.isAssignableFrom(messageClass))
                throw new IllegalArgumentException("Class provided should be a subtype of Message");

            parseFromMethod = messageClass.getMethod("parseFrom", byte[].class);
        } catch(NoSuchMethodException e) {
            throw new IllegalArgumentException("No parseFrom static method found, the provided class is not a Message.",
                                               e);
        } catch(ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private String getJavaFqnForMessage(String currentSchemaInfo) {
        if(StringUtils.isEmpty(currentSchemaInfo))
            throw new IllegalArgumentException("The type protobuf requires a non-empty schema-info.");

        String[] languagePairs = StringUtils.split(currentSchemaInfo, ',');
        if(languagePairs.length > 1)
            throw new IllegalArgumentException(ONLY_JAVA_CLIENTS_SUPPORTED);

        String[] javaPair = StringUtils.split(languagePairs[0], '=');
        if(javaPair.length != 2 || !javaPair[0].trim().equals("java"))
            throw new IllegalArgumentException(ONLY_JAVA_CLIENTS_SUPPORTED);

        return javaPair[1].trim();
    }

    public byte[] toBytes(T object) {
        return object.toByteArray();
    }

    public T toObject(byte[] bytes) {
        try {
            return messageClass.cast(parseFromMethod.invoke(null, bytes));
        } catch(InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        } catch(IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

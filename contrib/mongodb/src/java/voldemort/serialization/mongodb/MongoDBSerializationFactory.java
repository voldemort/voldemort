package voldemort.serialization.mongodb;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

/**
 * Serializer factory that handles MongoDB serialization as well as normal
 * serializers
 * 
 * @author jay
 * 
 */
public class MongoDBSerializationFactory extends DefaultSerializerFactory {

    private static final String MONGODOC_TYPE_NAME = "mongodoc";

    @Override
    public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
        String name = serializerDef.getName();
        if(name.equals(MONGODOC_TYPE_NAME)) {
            return new MongoDBDocSerializer();
        }
        else
            return super.getSerializer(serializerDef);
    }

}

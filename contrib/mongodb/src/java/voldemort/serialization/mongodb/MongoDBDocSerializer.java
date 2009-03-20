/**
 * See the NOTICE.txt file distributed with this work for information regarding
 * copyright ownership.
 * 
 * The authors license this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.serialization.mongodb;

import org.mongodb.driver.MongoDBException;
import org.mongodb.driver.impl.DirectBufferTLS;
import org.mongodb.driver.ts.Doc;
import org.mongodb.driver.util.BSONObject;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;

/**
 * Serializer for working with MongoDB Doc objects
 * 
 * @author geir
 */
public class MongoDBDocSerializer implements Serializer<Doc> {

    public MongoDBDocSerializer() {}

    public byte[] toBytes(Doc doc) {

        BSONObject bo = new BSONObject(getTLS().getWriteBuffer());

        try {
            bo.serialize(doc);
            return bo.toArray();
        } catch(MongoDBException e) {
            throw new SerializationException(e);
        }
    }

    public Doc toObject(byte[] bytes) {
        BSONObject bo = new BSONObject(getTLS().getReadBuffer());

        try {
            return bo.deserialize(bytes);
        } catch(MongoDBException e) {
            throw new SerializationException(e);
        }
    }

    private DirectBufferTLS getTLS() {
        DirectBufferTLS tls = DirectBufferTLS.getThreadLocal();
        if(tls == null) {
            tls = new DirectBufferTLS();
            tls.set();
        }

        return tls;
    }

}

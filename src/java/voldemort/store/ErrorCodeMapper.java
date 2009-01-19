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

package voldemort.store;

import voldemort.VoldemortException;
import voldemort.utils.ReflectUtils;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Map error codes to exceptions and vice versa
 * 
 * @author jay
 * 
 */
public class ErrorCodeMapper {

    private BiMap<Short, Class<? extends VoldemortException>> mapping;

    public ErrorCodeMapper() {
        mapping = new HashBiMap<Short, Class<? extends VoldemortException>>();
        mapping.put((short) 1, VoldemortException.class);
        mapping.put((short) 2, InsufficientOperationalNodesException.class);
        mapping.put((short) 3, StoreOperationFailureException.class);
        mapping.put((short) 4, ObsoleteVersionException.class);
        mapping.put((short) 6, UnknownFailure.class);
        mapping.put((short) 7, UnreachableStoreException.class);
        mapping.put((short) 8, InconsistentDataException.class);
    }

    public VoldemortException getError(short code, String message) {
        Class<? extends VoldemortException> klass = mapping.get(code);
        if(klass == null)
            return new UnknownFailure(Integer.toString(code));
        else
            return ReflectUtils.construct(klass, new Object[] { message });
    }

    public short getCode(VoldemortException e) {
        Short code = mapping.inverse().get(e.getClass());
        if(code == null)
            throw new IllegalArgumentException("No mapping code for " + e.getClass());
        else
            return code;
    }

}

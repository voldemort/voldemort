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

package voldemort.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utilities for reflection
 * 
 * @author jay
 * 
 */
public class ReflectUtils {

    public static <T> T construct(Class<T> klass, Object[] args) {
        Class<?>[] klasses = new Class[args.length];
        for(int i = 0; i < args.length; i++)
            klasses[i] = args[i].getClass();
        try {
            Constructor<T> cons = klass.getConstructor(klasses);
            T t = cons.newInstance(args);
            return t;
        } catch(NoSuchMethodException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args,
                                       e);
        } catch(InvocationTargetException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args,
                                       e);
        } catch(InstantiationException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args,
                                       e);
        } catch(IllegalAccessException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args,
                                       e);
        }
    }

    public static String getPropertyName(String name) {
        if(name != null && (name.startsWith("get") || name.startsWith("set"))) {
            StringBuilder b = new StringBuilder(name);
            b.delete(0, 3);
            b.setCharAt(0, Character.toLowerCase(b.charAt(0)));
            return b.toString();
        } else {
            return name;
        }
    }

}

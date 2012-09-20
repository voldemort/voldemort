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

import java.lang.annotation.Annotation;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.Descriptor;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.modelmbean.InvalidTargetObjectTypeException;
import javax.management.modelmbean.ModelMBean;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanConstructorInfo;
import javax.management.modelmbean.ModelMBeanInfo;
import javax.management.modelmbean.ModelMBeanInfoSupport;
import javax.management.modelmbean.ModelMBeanNotificationInfo;
import javax.management.modelmbean.ModelMBeanOperationInfo;
import javax.management.modelmbean.RequiredModelMBean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.annotations.jmx.JmxParam;
import voldemort.annotations.jmx.JmxSetter;

/**
 * JMX helper functions
 * 
 * These rely on annotations to create MBeans. Would be easier to just use the
 * annotations in Java 6, but we don't want to require Java 6 just for our own
 * convenience. Hence reinventing the wheel.
 * 
 * 
 */
public class JmxUtils {

    private static final Object LOCK = new Object();
    private static final Logger logger = Logger.getLogger(JmxUtils.class);

    public static final String MBEAN_NAME_SEPARATOR = "-";

    /**
     * Create a model mbean from an object using the description given in the
     * Jmx annotation if present. Only operations are supported so far, no
     * attributes, constructors, or notifications
     * 
     * @param o The object to create an MBean for
     * @return The ModelMBean for the given object
     */
    public static ModelMBean createModelMBean(Object o) {
        try {
            ModelMBean mbean = new RequiredModelMBean();
            JmxManaged annotation = o.getClass().getAnnotation(JmxManaged.class);
            String description = annotation == null ? "" : annotation.description();
            ModelMBeanInfo info = new ModelMBeanInfoSupport(o.getClass().getName(),
                                                            description,
                                                            extractAttributeInfo(o),
                                                            new ModelMBeanConstructorInfo[0],
                                                            extractOperationInfo(o),
                                                            new ModelMBeanNotificationInfo[0]);
            mbean.setModelMBeanInfo(info);
            mbean.setManagedResource(o, "ObjectReference");

            return mbean;
        } catch(MBeanException e) {
            throw new VoldemortException(e);
        } catch(InvalidTargetObjectTypeException e) {
            throw new VoldemortException(e);
        } catch(InstanceNotFoundException e) {
            throw new VoldemortException(e);
        }
    }

    /**
     * Extract all operations and attributes from the given object that have
     * been annotated with the Jmx annotation. Operations are all methods that
     * are marked with the JmxOperation annotation.
     * 
     * @param object The object to process
     * @return An array of operations taken from the object
     */
    public static ModelMBeanOperationInfo[] extractOperationInfo(Object object) {
        ArrayList<ModelMBeanOperationInfo> infos = new ArrayList<ModelMBeanOperationInfo>();
        for(Method m: object.getClass().getMethods()) {
            JmxOperation jmxOperation = m.getAnnotation(JmxOperation.class);
            JmxGetter jmxGetter = m.getAnnotation(JmxGetter.class);
            JmxSetter jmxSetter = m.getAnnotation(JmxSetter.class);
            if(jmxOperation != null || jmxGetter != null || jmxSetter != null) {
                String description = "";
                int visibility = 1;
                int impact = MBeanOperationInfo.UNKNOWN;
                if(jmxOperation != null) {
                    description = jmxOperation.description();
                    impact = jmxOperation.impact();
                } else if(jmxGetter != null) {
                    description = jmxGetter.description();
                    impact = MBeanOperationInfo.INFO;
                    visibility = 4;
                } else if(jmxSetter != null) {
                    description = jmxSetter.description();
                    impact = MBeanOperationInfo.ACTION;
                    visibility = 4;
                }
                ModelMBeanOperationInfo info = new ModelMBeanOperationInfo(m.getName(),
                                                                           description,
                                                                           extractParameterInfo(m),
                                                                           m.getReturnType()
                                                                            .getName(), impact);
                info.getDescriptor().setField("visibility", Integer.toString(visibility));
                infos.add(info);
            }
        }

        return infos.toArray(new ModelMBeanOperationInfo[infos.size()]);
    }

    /**
     * Extract all operations from the given object that have been annotated
     * with the Jmx annotation. Operations are all methods that are marked with
     * the JMX annotation and are not getters and setters (which are extracted
     * as attributes).
     * 
     * @param object The object to process
     * @return An array of attributes taken from the object
     */
    public static ModelMBeanAttributeInfo[] extractAttributeInfo(Object object) {
        Map<String, Method> getters = new HashMap<String, Method>();
        Map<String, Method> setters = new HashMap<String, Method>();
        Map<String, String> descriptions = new HashMap<String, String>();
        for(Method m: object.getClass().getMethods()) {
            JmxGetter getter = m.getAnnotation(JmxGetter.class);
            if(getter != null) {
                getters.put(getter.name(), m);
                descriptions.put(getter.name(), getter.description());
            }
            JmxSetter setter = m.getAnnotation(JmxSetter.class);
            if(setter != null) {
                setters.put(setter.name(), m);
                descriptions.put(setter.name(), setter.description());
            }
        }

        Set<String> attributes = new HashSet<String>(getters.keySet());
        attributes.addAll(setters.keySet());
        List<ModelMBeanAttributeInfo> infos = new ArrayList<ModelMBeanAttributeInfo>();
        for(String name: attributes) {
            try {
                Method getter = getters.get(name);
                Method setter = setters.get(name);
                ModelMBeanAttributeInfo info = new ModelMBeanAttributeInfo(name,
                                                                           descriptions.get(name),
                                                                           getter,
                                                                           setter);
                Descriptor descriptor = info.getDescriptor();
                if(getter != null)
                    descriptor.setField("getMethod", getter.getName());
                if(setter != null)
                    descriptor.setField("setMethod", setter.getName());
                info.setDescriptor(descriptor);
                infos.add(info);
            } catch(IntrospectionException e) {
                throw new VoldemortException(e);
            }
        }

        return infos.toArray(new ModelMBeanAttributeInfo[infos.size()]);
    }

    /**
     * Extract the parameters from a method using the Jmx annotation if present,
     * or just the raw types otherwise
     * 
     * @param m The method to extract parameters from
     * @return An array of parameter infos
     */
    public static MBeanParameterInfo[] extractParameterInfo(Method m) {
        Class<?>[] types = m.getParameterTypes();
        Annotation[][] annotations = m.getParameterAnnotations();
        MBeanParameterInfo[] params = new MBeanParameterInfo[types.length];
        for(int i = 0; i < params.length; i++) {
            boolean hasAnnotation = false;
            for(int j = 0; j < annotations[i].length; j++) {
                if(annotations[i][j] instanceof JmxParam) {
                    JmxParam param = (JmxParam) annotations[i][j];
                    params[i] = new MBeanParameterInfo(param.name(),
                                                       types[i].getName(),
                                                       param.description());
                    hasAnnotation = true;
                    break;
                }
            }
            if(!hasAnnotation) {
                params[i] = new MBeanParameterInfo("", types[i].getName(), "");
            }
        }

        return params;
    }

    /**
     * Create a JMX ObjectName
     * 
     * @param domain The domain of the object
     * @param type The type of the object
     * @return An ObjectName representing the name
     */
    public static ObjectName createObjectName(String domain, String type) {
        try {
            return new ObjectName(domain + ":type=" + type);
        } catch(MalformedObjectNameException e) {
            throw new VoldemortException(e);
        }
    }

    /**
     * Create an ObjectName from a class
     * 
     * @param c The class
     * @return The created object name
     */
    public static ObjectName createObjectName(Class<?> c) {
        return createObjectName(getPackageName(c), getClassName(c));
    }

    /**
     * Get the package for this class
     * 
     * @param c The class
     * @return The package name as a String
     */
    public static String getPackageName(Class<?> c) {
        String name = c.getName();
        return name.substring(0, name.lastIndexOf('.'));
    }

    /**
     * Get the class name without the package
     * 
     * @param c The class name with package
     * @return the class name without the package
     */
    public static String getClassName(Class<?> c) {
        String name = c.getName();
        return name.substring(name.lastIndexOf('.') + 1, name.length());
    }

    /**
     * Register the given mbean with the platform mbean server
     * 
     * @param mbean The mbean to register
     * @param name The name to register under
     */
    public static void registerMbean(Object mbean, ObjectName name) {
        registerMbean(ManagementFactory.getPlatformMBeanServer(),
                      JmxUtils.createModelMBean(mbean),
                      name);
    }

    /**
     * Register the given object under the package name of the object's class
     * with the given type name.
     * 
     * this method using the platform mbean server as returned by
     * ManagementFactory.getPlatformMBeanServer()
     * 
     * @param typeName The name of the type to register
     * @param obj The object to register as an mbean
     */
    public static ObjectName registerMbean(String typeName, Object obj) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = JmxUtils.createObjectName(JmxUtils.getPackageName(obj.getClass()),
                                                    typeName);
        registerMbean(server, JmxUtils.createModelMBean(obj), name);
        return name;
    }

    /**
     * Register the given mbean with the server
     * 
     * @param server The server to register with
     * @param mbean The mbean to register
     * @param name The name to register under
     */
    public static void registerMbean(MBeanServer server, ModelMBean mbean, ObjectName name) {
        try {
            synchronized(LOCK) {
                if(server.isRegistered(name))
                    JmxUtils.unregisterMbean(server, name);
                server.registerMBean(mbean, name);
            }
        } catch(Exception e) {
            logger.error("Error registering mbean:", e);
        }
    }

    /**
     * Unregister the mbean with the given name
     * 
     * @param server The server to unregister from
     * @param name The name of the mbean to unregister
     */
    public static void unregisterMbean(MBeanServer server, ObjectName name) {
        try {
            server.unregisterMBean(name);
        } catch(Exception e) {
            logger.error("Error unregistering mbean", e);
        }
    }

    /**
     * Unregister the mbean with the given name from the platform mbean server
     * 
     * @param name The name of the mbean to unregister
     */
    public static void unregisterMbean(ObjectName name) {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        } catch(Exception e) {
            logger.error("Error unregistering mbean", e);
        }
    }

    /**
     * Return the string representation of jmxId
     * 
     * @param jmxId
     * @return
     */
    public static String getJmxId(int jmxId) {
        return jmxId == 0 ? "" : Integer.toString(jmxId);
    }

}

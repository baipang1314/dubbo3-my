/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.beans.factory.ScopeBeanFactory;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.extension.ExtensionAccessor;
import org.apache.dubbo.common.extension.ExtensionDirector;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_UNABLE_DESTROY_MODEL;

/**
 * 模型对象的公共抽象父类型
 */
public abstract class ScopeModel implements ExtensionAccessor {
    protected static final ErrorTypeAwareLogger LOGGER = LoggerFactory.getErrorTypeAwareLogger(ScopeModel.class);

    /**
     * The internal id is used to represent the hierarchy of the model tree, such as:
     * 内部 ID 用于表示模型树的层次结构
     * <ol>
     *     <li>1</li>
     *     FrameworkModel (index=1)
     *     <li>1.2</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2)
     *     <li>1.2.0</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2) -> ModuleModel (index=0, internal module)
     *     <li>1.2.1</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2) -> ModuleModel (index=1, first user module)
     * </ol>
     * FrameworkModel    1
     * ApplicationModel  1.1
     * ModuleModel       1.1.1
     */
    private String internalId;

    /**
     * Public Model Name, can be set from user
     */
    private String modelName;

    private String desc;

    private final Set<ClassLoader> classLoaders = new ConcurrentHashSet<>();

    private final ScopeModel parent;
    private final ExtensionScope scope;

    private volatile ExtensionDirector extensionDirector;

    /**
     * 一个内部共享的Bean工厂
     */
    private volatile ScopeBeanFactory beanFactory;
    private final List<ScopeModelDestroyListener> destroyListeners = new CopyOnWriteArrayList<>();

    private final List<ScopeClassLoaderListener> classLoaderListeners = new CopyOnWriteArrayList<>();

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    private final boolean internalScope;

    /**
     * 创建锁，同一时间，只能创建FrameworkModel,ApplicationModel,ModuleModel中的一个
     */
    protected final Object instLock = new Object();

    protected ScopeModel(ScopeModel parent, ExtensionScope scope, boolean isInternal) {
        this.parent = parent;
        this.scope = scope;
        this.internalScope = isInternal;
    }

    /**
     * NOTE:
     * <ol>
     *  <li>The initialize method only be called in subclass.</li>
     *  initialize 方法只能在 subclass（子类:FrameworkModel,ApplicationModel,ModuleModel）中调用,使用子类都要执行这个方法进行初始化
     * <li>
     * In subclass, the extensionDirector and beanFactory are available in initialize but not available in constructor.
     * 在子类中，extensionDirector 和 beanFactory 在 initialize 中可用，但在 constructor 中不可用。
     * </li>
     * </ol>
     */
    protected void initialize() {
        synchronized (instLock) {
            // 创建 ExtensionDirector（如果有parent，创建ExtensionDirector时给ExtensionDirector的parent赋值）
            //初始化ExtensionDirector是一个作用域扩展加载程序管理器。
            //ExtensionDirector支持多个级别，子级可以继承父级的扩展实例。
            //查找和创建扩展实例的方法类似于Java classloader。
            this.extensionDirector =
                    new ExtensionDirector(parent != null ? parent.getExtensionDirector() : null, scope, this);
            //这个参考了Spring的生命周期回调思想，添加一个扩展初始化的前后调用的处理器，在扩展初始化之前或之后调用的后处理器，参数类型为ExtensionPostProcessor
            this.extensionDirector.addExtensionPostProcessor(new ScopeModelAwareExtensionProcessor(this));
            // 创建 BeanFactory（如果有parent，创建ScopeBeanFactory时给ScopeBeanFactory赋值）
            // 一个内部共享的域工厂对象，用于注册Bean，创建Bean，获取Bean，初始化Bean等
            this.beanFactory = new ScopeBeanFactory(parent != null ? parent.getBeanFactory() : null, extensionDirector);

            // Add Framework's ClassLoader by default
            //将当前类的加载器存入加载器集合classLoaders中
            ClassLoader dubboClassLoader = ScopeModel.class.getClassLoader();
            if (dubboClassLoader != null) {
                this.addClassLoader(dubboClassLoader);
            }
        }
    }

    protected abstract Lock acquireDestroyLock();

    public void destroy() {
        Lock lock = acquireDestroyLock();
        try {
            lock.lock();
            // aqs
            if (destroyed.compareAndSet(false, true)) {
                try {
                    onDestroy();
                    HashSet<ClassLoader> copyOfClassLoaders = new HashSet<>(classLoaders);
                    for (ClassLoader classLoader : copyOfClassLoaders) {
                        removeClassLoader(classLoader);
                    }
                    if (beanFactory != null) {
                        beanFactory.destroy();
                    }
                    if (extensionDirector != null) {
                        extensionDirector.destroy();
                    }
                } catch (Throwable t) {
                    LOGGER.error(CONFIG_UNABLE_DESTROY_MODEL, "", "", "Error happened when destroying ScopeModel.", t);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    protected void notifyDestroy() {
        for (ScopeModelDestroyListener destroyListener : destroyListeners) {
            destroyListener.onDestroy(this);
        }
    }

    protected void notifyProtocolDestroy() {
        for (ScopeModelDestroyListener destroyListener : destroyListeners) {
            if (destroyListener.isProtocol()) {
                destroyListener.onDestroy(this);
            }
        }
    }

    protected void notifyClassLoaderAdd(ClassLoader classLoader) {
        for (ScopeClassLoaderListener classLoaderListener : classLoaderListeners) {
            classLoaderListener.onAddClassLoader(this, classLoader);
        }
    }

    protected void notifyClassLoaderDestroy(ClassLoader classLoader) {
        for (ScopeClassLoaderListener classLoaderListener : classLoaderListeners) {
            classLoaderListener.onRemoveClassLoader(this, classLoader);
        }
    }

    protected abstract void onDestroy();

    public final void addDestroyListener(ScopeModelDestroyListener listener) {
        destroyListeners.add(listener);
    }

    public final void addClassLoaderListener(ScopeClassLoaderListener listener) {
        classLoaderListeners.add(listener);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public <T> T getAttribute(String key, Class<T> type) {
        return (T) attributes.get(key);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public ExtensionDirector getExtensionDirector() {
        return extensionDirector;
    }

    public ScopeBeanFactory getBeanFactory() {
        return beanFactory;
    }

    public ScopeModel getParent() {
        return parent;
    }

    public ExtensionScope getScope() {
        return scope;
    }

    public void addClassLoader(ClassLoader classLoader) {
        synchronized (instLock) {
            this.classLoaders.add(classLoader);
            if (parent != null) {
                parent.addClassLoader(classLoader);
            }
            extensionDirector.removeAllCachedLoader();
            notifyClassLoaderAdd(classLoader);
        }
    }

    public void removeClassLoader(ClassLoader classLoader) {
        synchronized (instLock) {
            if (checkIfClassLoaderCanRemoved(classLoader)) {
                this.classLoaders.remove(classLoader);
                if (parent != null) {
                    parent.removeClassLoader(classLoader);
                }
                extensionDirector.removeAllCachedLoader();
                notifyClassLoaderDestroy(classLoader);
            }
        }
    }

    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return classLoader != null && !classLoader.equals(ScopeModel.class.getClassLoader());
    }

    public Set<ClassLoader> getClassLoaders() {
        return Collections.unmodifiableSet(classLoaders);
    }

    /**
     * Get current model's environment.
     * 获取当前模型的环境
     * </br>
     * Note: This method should not start with `get` or it would be invoked due to Spring boot refresh.
     * 此方法不应以 'get' 开头，否则会因 Spring 启动刷新而被调用。
     * @see <a href="https://github.com/apache/dubbo/issues/12542">Configuration refresh issue</a>
     */
    public abstract Environment modelEnvironment();

    /**
     * Get current model's environment.
     *
     * @see <a href="https://github.com/apache/dubbo/issues/12542">Configuration refresh issue</a>
     * @deprecated use modelEnvironment() instead
     */
    @Deprecated
    public final Environment getModelEnvironment() {
        try {
            return modelEnvironment();
        } catch (Exception ex) {
            return null;
        }
    }

    public String getInternalId() {
        return this.internalId;
    }

    void setInternalId(String internalId) {
        this.internalId = internalId;
    }

    protected String buildInternalId(String parentInternalId, long childIndex) {
        // FrameworkModel    1
        // ApplicationModel  1.1
        // ModuleModel       1.1.1
        if (StringUtils.hasText(parentInternalId)) {
            return parentInternalId + "." + childIndex;
        } else {
            return "" + childIndex;
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
        this.desc = buildDesc();
    }

    public boolean isInternal() {
        return internalScope;
    }

    /**
     * @return to describe string of this scope model
     */
    public String getDesc() {
        if (this.desc == null) {
            this.desc = buildDesc();
        }
        return this.desc;
    }

    private String buildDesc() {
        // Dubbo Framework[1]
        // Dubbo Application[1.1](appName)
        // Dubbo Module[1.1.1](appName/moduleName)
        String type = this.getClass().getSimpleName().replace("Model", "");
        String desc = "Dubbo " + type + "[" + this.getInternalId() + "]";

        // append model name path
        String modelNamePath = this.getModelNamePath();
        if (StringUtils.hasText(modelNamePath)) {
            desc += "(" + modelNamePath + ")";
        }
        return desc;
    }

    private String getModelNamePath() {
        if (this instanceof ApplicationModel) {
            return safeGetAppName((ApplicationModel) this);
        } else if (this instanceof ModuleModel) {
            String modelName = this.getModelName();
            if (StringUtils.hasText(modelName)) {
                // appName/moduleName
                return safeGetAppName(((ModuleModel) this).getApplicationModel()) + "/" + modelName;
            }
        }
        return null;
    }

    private static String safeGetAppName(ApplicationModel applicationModel) {
        String modelName = applicationModel.getModelName();
        if (StringUtils.isBlank(modelName)) {
            modelName = "unknown"; // unknown application
        }
        return modelName;
    }

    @Override
    public String toString() {
        return getDesc();
    }
}

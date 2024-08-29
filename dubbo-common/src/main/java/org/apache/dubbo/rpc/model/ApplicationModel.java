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

import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.context.ApplicationExt;
import org.apache.dubbo.common.deploy.ApplicationDeployer;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.context.ConfigManager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * {@link ExtensionLoader}, {@code DubboBootstrap} and this class are at present designed to be
 * singleton or static (by itself totally static or uses some static fields). So the instances
 * returned from them are of process scope. If you want to support multiple dubbo servers in one
 * single process, you may need to refactor those three classes.
 * ExtensionLoader、DubboBootstrap和这个类目前被设计为单例或静态的（本身完全静态或使用一些静态字段）。
 * 因此，从它们返回的实例属于进程范围。
 * 如果想在单个进程中支持多个 dubbo 服务器，可能需要重构这三个类。
 * <p>
 * Represent an application which is using Dubbo and store basic metadata info for using
 * during the processing of RPC invoking.
 * 代表一个正在使用 Dubbo 的应用程序，并存储基本的元数据信息，以便在 RPC 调用过程中使用。(从角色上看，要么是提供者要么是消费者)
 * <p>
 * ApplicationModel includes many ProviderModel which is about published services
 * and many Consumer Model which is about subscribed services.
 * ApplicationModel 包括许多 ProviderModel（与已发布的服务有关）和许多 Consumer Model（与订阅的服务有关）。
 * <p>
 */
public class ApplicationModel extends ScopeModel {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ApplicationModel.class);
    public static final String NAME = "ApplicationModel";
    private final List<ModuleModel> moduleModels = new CopyOnWriteArrayList<>();
    private final List<ModuleModel> pubModuleModels = new CopyOnWriteArrayList<>();
    /**
     * 环境信息
     */
    private volatile Environment environment;
    /**
     * 配置管理实例对象
     */
    private volatile ConfigManager configManager;
    private volatile ServiceRepository serviceRepository;
    /**
     * 应用程序部署器
     */
    private volatile ApplicationDeployer deployer;

    /**
     * 所属框架实例
     */
    private final FrameworkModel frameworkModel;

    private final ModuleModel internalModule;

    private volatile ModuleModel defaultModule;

    // internal module index is 0, default module index is 1
    private final AtomicInteger moduleIndex = new AtomicInteger(0);

    // --------- static methods ----------//

    public static ApplicationModel ofNullable(ApplicationModel applicationModel) {
        if (applicationModel != null) {
            return applicationModel;
        } else {
            return defaultModel();
        }
    }

    /**
     * During destroying the default FrameworkModel, the FrameworkModel.defaultModel() or ApplicationModel.defaultModel()
     * will return a broken model, maybe cause unpredictable problem.
     * Recommendation: Avoid using the default model as much as possible.
     * 在销毁默认的 FrameworkModel 时， FrameworkModel.defaultModel()或ApplicationModel.defaultModel()
     * 将返回一个损坏的模型可能会导致不可预知的问题。
     * 建议：尽量避免使用默认模型。
     *
     * @return the global default ApplicationModel
     */
    public static ApplicationModel defaultModel() {
        // should get from default FrameworkModel, avoid out of sync
        // FrameworkModel.defaultModel()可能返回空
        return FrameworkModel.defaultModel().defaultApplication();
    }

    // ------------- instance methods ---------------//

    protected ApplicationModel(FrameworkModel frameworkModel) {
        this(frameworkModel, false);
    }

    protected ApplicationModel(FrameworkModel frameworkModel, boolean isInternal) {
        // 创建ApplicationModel需要传入所属的FrameworkModel和是否是内部isInternal，isInternal如果不传，则默认为false
        super(frameworkModel, ExtensionScope.APPLICATION, isInternal);
        // 父类ScopeModel的锁（一个属性Object）
        synchronized (instLock) {
            // 多线程环境下，如果是通过DefaultModel获取的FrameworkModel，可能会返回已经损毁的FrameworkModel，可能是个null
            Assert.notNull(frameworkModel, "FrameworkModel can not be null");
            //应用程序域成员变量记录frameworkModel对象
            this.frameworkModel = frameworkModel;
            //frameworkModel对象添加当前应用程序域对象
            frameworkModel.addApplication(this);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(getDesc() + " is created");
            }
            // 调用父类公用的初始化方法
            initialize();

            // ApplicationModel初始化时会创建一个内部的modelModel(上层模型初始化都会创建一个内部的下一层模型，FrameworkModel会创建一个内部的ApplicationModel)
            // TODO 这个内部的下层模型的作用是什么？Framework中的ApplicationModel用来获取应用配置管理器并且设置了一些记录着常见的应用配置信息
            this.internalModule = new ModuleModel(this, true);
            //创建一个独立服务存储对象
            this.serviceRepository = new ServiceRepository(this);

            //获取应用程序初始化监听器ApplicationInitListener扩展（当前版本ApplicationInitListener暂时还没有实现类）
            ExtensionLoader<ApplicationInitListener> extensionLoader =
                    this.getExtensionLoader(ApplicationInitListener.class);
            Set<String> listenerNames = extensionLoader.getSupportedExtensions();
            for (String listenerName : listenerNames) {
                extensionLoader.getExtension(listenerName).init();
            }
            //初始化扩展(这个是应用程序生命周期的方法调用，这里调用初始化方法
            initApplicationExts();

            //获取域模型初始化器扩展对象列表，然后执行初始化方法
            ExtensionLoader<ScopeModelInitializer> initializerExtensionLoader =
                    this.getExtensionLoader(ScopeModelInitializer.class);
            Set<ScopeModelInitializer> initializers = initializerExtensionLoader.getSupportedExtensionInstances();
            for (ScopeModelInitializer initializer : initializers) {
                //FrameworkModel执行initializeFrameworkModel
                initializer.initializeApplicationModel(this);
            }

            Assert.notNull(getApplicationServiceRepository(), "ApplicationServiceRepository can not be null");
            Assert.notNull(getApplicationConfigManager(), "ApplicationConfigManager can not be null");
            Assert.assertTrue(
                    getApplicationConfigManager().isInitialized(), "ApplicationConfigManager can not be initialized");
        }
    }

    // already synchronized in constructor
    private void initApplicationExts() {
        Set<ApplicationExt> exts = this.getExtensionLoader(ApplicationExt.class).getSupportedExtensionInstances();
        for (ApplicationExt ext : exts) {
            // ModuleExt从父类Lifecycle继承了initialize方法
            ext.initialize();
        }
    }

    @Override
    protected void onDestroy() {
        synchronized (instLock) {
            // 1. remove from frameworkModel
            frameworkModel.removeApplication(this);

            // 2. pre-destroy, set stopping
            if (deployer != null) {
                // destroy registries and unregister services from registries first to notify consumers to stop
                // consuming this instance.
                deployer.preDestroy();
            }

            // 3. Try to destroy protocols to stop this instance from receiving new requests from connections
            frameworkModel.tryDestroyProtocols();

            // 4. destroy application resources
            for (ModuleModel moduleModel : moduleModels) {
                if (moduleModel != internalModule) {
                    moduleModel.destroy();
                }
            }
            // 5. destroy internal module later
            internalModule.destroy();

            // 6. post-destroy, release registry resources
            if (deployer != null) {
                deployer.postDestroy();
            }

            // 7. destroy other resources (e.g. ZookeeperTransporter )
            notifyDestroy();

            if (environment != null) {
                environment.destroy();
                environment = null;
            }
            if (configManager != null) {
                configManager.destroy();
                configManager = null;
            }
            if (serviceRepository != null) {
                serviceRepository.destroy();
                serviceRepository = null;
            }

            // 8. destroy framework if none application
            frameworkModel.tryDestroy();
        }
    }

    public FrameworkModel getFrameworkModel() {
        return frameworkModel;
    }

    public ModuleModel newModule() {
        synchronized (instLock) {
            return new ModuleModel(this);
        }
    }

    @Override
    public Environment modelEnvironment() {
        if (environment == null) {
            environment =
                    (Environment) this.getExtensionLoader(ApplicationExt.class).getExtension(Environment.NAME);
        }
        return environment;
    }

    public ConfigManager getApplicationConfigManager() {
        if (configManager == null) {
            configManager = (ConfigManager)
                    this.getExtensionLoader(ApplicationExt.class).getExtension(ConfigManager.NAME);
        }
        return configManager;
    }

    public ServiceRepository getApplicationServiceRepository() {
        return serviceRepository;
    }

    public ExecutorRepository getApplicationExecutorRepository() {
        return ExecutorRepository.getInstance(this);
    }

    public boolean NotExistApplicationConfig() {
        return !getApplicationConfigManager().getApplication().isPresent();
    }

    public ApplicationConfig getCurrentConfig() {
        return getApplicationConfigManager().getApplicationOrElseThrow();
    }

    public String getApplicationName() {
        return getCurrentConfig().getName();
    }

    public String tryGetApplicationName() {
        Optional<ApplicationConfig> appCfgOptional =
                getApplicationConfigManager().getApplication();
        return appCfgOptional.isPresent() ? appCfgOptional.get().getName() : null;
    }

    /**
     * 和frameworkModel.addApplication()方法类似
     */
    void addModule(ModuleModel moduleModel, boolean isInternal) {
        synchronized (instLock) {
            if (!this.moduleModels.contains(moduleModel)) {
                checkDestroyed();
                this.moduleModels.add(moduleModel);
                moduleModel.setInternalId(buildInternalId(getInternalId(), moduleIndex.getAndIncrement()));
                if (!isInternal) {
                    pubModuleModels.add(moduleModel);
                }
            }
        }
    }

    public void removeModule(ModuleModel moduleModel) {
        synchronized (instLock) {
            this.moduleModels.remove(moduleModel);
            this.pubModuleModels.remove(moduleModel);
            if (moduleModel == defaultModule) {
                defaultModule = findDefaultModule();
            }
        }
    }

    void tryDestroy() {
        synchronized (instLock) {
            if (this.moduleModels.isEmpty()
                    || (this.moduleModels.size() == 1 && this.moduleModels.get(0) == internalModule)) {
                destroy();
            }
        }
    }

    private void checkDestroyed() {
        if (isDestroyed()) {
            throw new IllegalStateException("ApplicationModel is destroyed");
        }
    }

    public List<ModuleModel> getModuleModels() {
        return Collections.unmodifiableList(moduleModels);
    }

    public List<ModuleModel> getPubModuleModels() {
        return Collections.unmodifiableList(pubModuleModels);
    }

    public ModuleModel getDefaultModule() {
        if (defaultModule == null) {
            synchronized (instLock) {
                if (defaultModule == null) {
                    defaultModule = findDefaultModule();
                    if (defaultModule == null) {
                        defaultModule = this.newModule();
                    }
                }
            }
        }
        return defaultModule;
    }

    private ModuleModel findDefaultModule() {
        synchronized (instLock) {
            for (ModuleModel moduleModel : moduleModels) {
                if (moduleModel != internalModule) {
                    return moduleModel;
                }
            }
            return null;
        }
    }

    public ModuleModel getInternalModule() {
        return internalModule;
    }

    @Override
    public void addClassLoader(ClassLoader classLoader) {
        super.addClassLoader(classLoader);
        if (environment != null) {
            environment.refreshClassLoaders();
        }
    }

    @Override
    public void removeClassLoader(ClassLoader classLoader) {
        super.removeClassLoader(classLoader);
        if (environment != null) {
            environment.refreshClassLoaders();
        }
    }

    @Override
    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return super.checkIfClassLoaderCanRemoved(classLoader) && !containsClassLoader(classLoader);
    }

    protected boolean containsClassLoader(ClassLoader classLoader) {
        return moduleModels.stream()
                .anyMatch(moduleModel -> moduleModel.getClassLoaders().contains(classLoader));
    }

    public ApplicationDeployer getDeployer() {
        return deployer;
    }

    public void setDeployer(ApplicationDeployer deployer) {
        this.deployer = deployer;
    }

    @Override
    protected Lock acquireDestroyLock() {
        return frameworkModel.acquireDestroyLock();
    }

    // =============================== Deprecated Methods Start =======================================

    /**
     * @deprecated use {@link ServiceRepository#allConsumerModels()}
     */
    @Deprecated
    public static Collection<ConsumerModel> allConsumerModels() {
        return defaultModel().getApplicationServiceRepository().allConsumerModels();
    }

    /**
     * @deprecated use {@link ServiceRepository#allProviderModels()}
     */
    @Deprecated
    public static Collection<ProviderModel> allProviderModels() {
        return defaultModel().getApplicationServiceRepository().allProviderModels();
    }

    /**
     * @deprecated use {@link FrameworkServiceRepository#lookupExportedService(String)}
     */
    @Deprecated
    public static ProviderModel getProviderModel(String serviceKey) {
        return defaultModel().getDefaultModule().getServiceRepository().lookupExportedService(serviceKey);
    }

    /**
     * @deprecated ConsumerModel should fetch from context
     */
    @Deprecated
    public static ConsumerModel getConsumerModel(String serviceKey) {
        return defaultModel().getDefaultModule().getServiceRepository().lookupReferredService(serviceKey);
    }

    /**
     * @deprecated Replace to {@link ScopeModel#modelEnvironment()}
     */
    @Deprecated
    public static Environment getEnvironment() {
        return defaultModel().modelEnvironment();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationConfigManager()}
     */
    @Deprecated
    public static ConfigManager getConfigManager() {
        return defaultModel().getApplicationConfigManager();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationServiceRepository()}
     */
    @Deprecated
    public static ServiceRepository getServiceRepository() {
        return defaultModel().getApplicationServiceRepository();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationExecutorRepository()}
     */
    @Deprecated
    public static ExecutorRepository getExecutorRepository() {
        return defaultModel().getApplicationExecutorRepository();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getCurrentConfig()}
     */
    @Deprecated
    public static ApplicationConfig getApplicationConfig() {
        return defaultModel().getCurrentConfig();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationName()}
     */
    @Deprecated
    public static String getName() {
        return defaultModel().getCurrentConfig().getName();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationName()}
     */
    @Deprecated
    public static String getApplication() {
        return getName();
    }

    // only for unit test
    @Deprecated
    public static void reset() {
        if (FrameworkModel.defaultModel().getDefaultAppModel() != null) {
            FrameworkModel.defaultModel().getDefaultAppModel().destroy();
        }
    }

    /**
     * @deprecated only for ut
     */
    @Deprecated
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /**
     * @deprecated only for ut
     */
    @Deprecated
    public void setConfigManager(ConfigManager configManager) {
        this.configManager = configManager;
    }

    /**
     * @deprecated only for ut
     */
    @Deprecated
    public void setServiceRepository(ServiceRepository serviceRepository) {
        this.serviceRepository = serviceRepository;
    }

    // =============================== Deprecated Methods End =======================================
}

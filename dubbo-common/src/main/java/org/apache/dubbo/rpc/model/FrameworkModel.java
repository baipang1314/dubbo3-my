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
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.resource.GlobalResourcesRepository;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.metadata.definition.TypeDefinitionBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Model of dubbo framework, it can be shared with multiple applications.
 * dubbo框架模型，可与多个应用程序共享
 */
public class FrameworkModel extends ScopeModel {

    // ========================= Static Fields Start ===================================

    protected static final Logger LOGGER = LoggerFactory.getLogger(FrameworkModel.class);

    public static final String NAME = "FrameworkModel";
    private static final AtomicLong index = new AtomicLong(1);

    private static final Object globalLock = new Object();

    private static volatile FrameworkModel defaultInstance;

    private static final List<FrameworkModel> allInstances = new CopyOnWriteArrayList<>();

    // ========================= Static Fields End ===================================

    // internal app index is 0, default app index is 1
    private final AtomicLong appIndex = new AtomicLong(0);

    private volatile ApplicationModel defaultAppModel;

    /**
     * 全部ApplicationModels，所有的ApplicationModel都有InternalId
     *
     */
    private final List<ApplicationModel> applicationModels = new CopyOnWriteArrayList<>();

    /**
     * 冗余的公开的ApplicationModels
     */
    private final List<ApplicationModel> pubApplicationModels = new CopyOnWriteArrayList<>();

    private final FrameworkServiceRepository serviceRepository;

    private final ApplicationModel internalApplicationModel;

    private final ReentrantLock destroyLock = new ReentrantLock();

    /**
     * Use {@link FrameworkModel#newModel()} to create a new model
     */
    public FrameworkModel() {
        // 父类的构造方法，对父类的parent、scope、isInternal属性赋值，初始化
        // 第一个参数null，表示这是一个顶级的域模型，
        // 第二个参数ExtensionScope.FRAMEWORK，表示这是一个框架域模型，
        // 第三个参数false，表示这是不是一个内部域模型
        super(null, ExtensionScope.FRAMEWORK, false);
        synchronized (globalLock) {
            synchronized (instLock) {
                //内部id用于表示模型树的层次结构，如层次结构:
                //FrameworkModel（索引=1）->ApplicationModel（索引=2）->ModuleModel（索引=1，第一个用户模块）
                //这个index变量是static类型的为静态全局变量默认值从1开始，如果有多个框架模型对象则internalId编号从1开始依次递增
                this.setInternalId(String.valueOf(index.getAndIncrement()));
                // register FrameworkModel instance early
                // 尽早将当前新创建的框架实例对象添加到缓存容器中
                allInstances.add(this);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(getDesc() + " is created");
                }
                //初始化框架模型领域对象，调用了在父类的初始化方法
                initialize();

                //使用TypeDefinitionBuilder的静态方法initBuilders来初始化类型构建器TypeBuilder类型集合
                TypeDefinitionBuilder.initBuilders(this);

                // 创建一个serviceRepository，赋值给serviceRepository属性
                // 框架服务存储仓库对象可以用于快速查询服务提供者信息
                serviceRepository = new FrameworkServiceRepository(this);

                //获取ScopeModelInitializer类型(域模型初始化器)的扩展加载器ExtensionLoader，每个扩展类型都会创建一个扩展加载器缓存起来
                ExtensionLoader<ScopeModelInitializer> initializerExtensionLoader =
                        this.getExtensionLoader(ScopeModelInitializer.class);
                //获取ScopeModelInitializer类型的支持的扩展集合，这里当前版本存在15个扩展类型实现
                Set<ScopeModelInitializer> initializers = initializerExtensionLoader.getSupportedExtensionInstances();
                //遍历这些扩展实现调用他们的initializeFrameworkModel方法来传递FrameworkModel类型对象
                for (ScopeModelInitializer initializer : initializers) {
                    // initializeFrameworkModeld的实现有通过FrameworkModel获取BeanFactory，来注册Bean、有给FrameworkModel添加监听器等
                    initializer.initializeFrameworkModel(this);
                }

                // 创建一个内部的ApplicationModel类型(上层模型初始化都会创建一个内部的下一层模型)
                internalApplicationModel = new ApplicationModel(this, true);
                //传递内部的应用程序模型对象internalApplicationModel和名称，以创建ApplicationConfig对象
                //通过internalApplicationModel对象获取ConfigManager类型对象，然后设置添加刚刚创建的ApplicationConfig对象
                //应用配置记录着常见的应用配置信息
                internalApplicationModel
                        .getApplicationConfigManager()
                        .setApplication(new ApplicationConfig(
                                internalApplicationModel, CommonConstants.DUBBO_INTERNAL_APPLICATION));
                //设置模块名
                internalApplicationModel.setModelName(CommonConstants.DUBBO_INTERNAL_APPLICATION);
            }
        }
    }

    @Override
    protected void onDestroy() {
        synchronized (instLock) {
            if (defaultInstance == this) {
                // NOTE: During destroying the default FrameworkModel, the FrameworkModel.defaultModel() or
                // ApplicationModel.defaultModel()
                // will return a broken model, maybe cause unpredictable problem.
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Destroying default framework model: " + getDesc());
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(getDesc() + " is destroying ...");
            }

            // destroy all application model
            for (ApplicationModel applicationModel : new ArrayList<>(applicationModels)) {
                applicationModel.destroy();
            }
            // check whether all application models are destroyed
            checkApplicationDestroy();

            // notify destroy and clean framework resources
            // see org.apache.dubbo.config.deploy.FrameworkModelCleaner
            notifyDestroy();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(getDesc() + " is destroyed");
            }

            // remove from allInstances and reset default FrameworkModel
            synchronized (globalLock) {
                allInstances.remove(this);
                resetDefaultFrameworkModel();
            }

            // if all FrameworkModels are destroyed, clean global static resources, shutdown dubbo completely
            destroyGlobalResources();
        }
    }

    private void checkApplicationDestroy() {
        synchronized (instLock) {
            if (applicationModels.size() > 0) {
                List<String> remainApplications =
                        applicationModels.stream().map(ScopeModel::getDesc).collect(Collectors.toList());
                throw new IllegalStateException(
                        "Not all application models are completely destroyed, remaining " + remainApplications.size()
                                + " application models may be created during destruction: " + remainApplications);
            }
        }
    }

    private void destroyGlobalResources() {
        synchronized (globalLock) {
            if (allInstances.isEmpty()) {
                GlobalResourcesRepository.getInstance().destroy();
            }
        }
    }

    /**
     * During destroying the default FrameworkModel, the FrameworkModel.defaultModel() or ApplicationModel.defaultModel()
     * will return a broken model, maybe cause unpredictable problem.
     * (多线程环境下)在销毁默认的 FrameworkModel 时，FrameworkModel.defaultModel（） 或 ApplicationModel.defaultModel（） 会返回一个损坏的模型，
     * 可能会导致不可预知的问题。 尽可能避免使用defaultModel
     * Recommendation: Avoid using the default model as much as possible.
     * @return the global default FrameworkModel
     */
    public static FrameworkModel defaultModel() {
        //双重校验锁的形式创建单例对象
        FrameworkModel instance = defaultInstance;
        if (instance == null) {
            synchronized (globalLock) {
                //重置默认框架模型
                resetDefaultFrameworkModel();
                if (defaultInstance == null) {
                    // 如果没有实例，创建实例，赋值返回
                    defaultInstance = new FrameworkModel();
                }
                instance = defaultInstance;
            }
        }
        Assert.notNull(instance, "Default FrameworkModel is null");
        return instance;
    }

    /**
     * Get all framework model instances
     * @return
     */
    public static List<FrameworkModel> getAllInstances() {
        synchronized (globalLock) {
            return Collections.unmodifiableList(new ArrayList<>(allInstances));
        }
    }

    /**
     * Destroy all framework model instances, shutdown dubbo engine completely.
     */
    public static void destroyAll() {
        synchronized (globalLock) {
            for (FrameworkModel frameworkModel : new ArrayList<>(allInstances)) {
                frameworkModel.destroy();
            }
        }
    }

    public ApplicationModel newApplication() {
        synchronized (instLock) {
            return new ApplicationModel(this);
        }
    }

    /**
     * Get or create default application model
     * @return
     */
    public ApplicationModel defaultApplication() {
        ApplicationModel appModel = this.defaultAppModel;
        if (appModel == null) {
            // check destroyed before acquire inst lock, avoid blocking during destroying
            checkDestroyed();
            resetDefaultAppModel();
            if ((appModel = this.defaultAppModel) == null) {
                synchronized (instLock) {
                    if (this.defaultAppModel == null) {
                        this.defaultAppModel = newApplication();
                    }
                    appModel = this.defaultAppModel;
                }
            }
        }
        Assert.notNull(appModel, "Default ApplicationModel is null");
        return appModel;
    }

    ApplicationModel getDefaultAppModel() {
        return defaultAppModel;
    }

    /***
     * 把Application放到FrameworkModel的缓存里面
     *
     * ApplicationModel初始化的时候，调用这个方法去把新建的Application放到FrameworkModel的缓存里面
     * 对象是否已经被标记为销毁状态，如果是正在销毁的FrameworkModel，会添加失败，并且报错。销毁FrameworkModel的行为一个无法回滚
     */
    void addApplication(ApplicationModel applicationModel) {
        // can not add new application if it's destroying
        checkDestroyed();
        synchronized (instLock) {
            if (!this.applicationModels.contains(applicationModel)) {
                applicationModel.setInternalId(buildInternalId(getInternalId(), appIndex.getAndIncrement()));
                // 全部加到总的应用模型集合applicationModels中
                this.applicationModels.add(applicationModel);
                //如果非内部的则也向公开应用模型集合pubApplicationModels中添加一下
                if (!applicationModel.isInternal()) {
                    this.pubApplicationModels.add(applicationModel);
                }
            }
        }
    }

    void removeApplication(ApplicationModel model) {
        synchronized (instLock) {
            this.applicationModels.remove(model);
            if (!model.isInternal()) {
                this.pubApplicationModels.remove(model);
            }
            resetDefaultAppModel();
        }
    }

    /**
     * Protocols are special resources that need to be destroyed as soon as possible.
     *
     * Since connections inside protocol are not classified by applications, trying to destroy protocols in advance might only work for singleton application scenario.
     */
    void tryDestroyProtocols() {
        synchronized (instLock) {
            if (pubApplicationModels.size() == 0) {
                notifyProtocolDestroy();
            }
        }
    }

    void tryDestroy() {
        synchronized (instLock) {
            if (pubApplicationModels.size() == 0) {
                destroy();
            }
        }
    }

    private void checkDestroyed() {
        if (isDestroyed()) {
            throw new IllegalStateException("FrameworkModel is destroyed");
        }
    }

    private void resetDefaultAppModel() {
        synchronized (instLock) {
            if (this.defaultAppModel != null && !this.defaultAppModel.isDestroyed()) {
                return;
            }
            //取第一个公开的应用模型做为默认应用模型
            ApplicationModel oldDefaultAppModel = this.defaultAppModel;
            if (pubApplicationModels.size() > 0) {
                this.defaultAppModel = pubApplicationModels.get(0);
            } else {
                this.defaultAppModel = null;
            }
            if (defaultInstance == this && oldDefaultAppModel != this.defaultAppModel) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Reset global default application from " + safeGetModelDesc(oldDefaultAppModel) + " to "
                            + safeGetModelDesc(this.defaultAppModel));
                }
            }
        }
    }

    private static void resetDefaultFrameworkModel() {
        //全局悲观锁，同一个时刻只能有一个线程执行重置操作
        synchronized (globalLock) {
            //defaultInstance为当前成员变量 FrameworkModel类型 代表当前默认的FrameworkModel类型的实例对象
            if (defaultInstance != null && !defaultInstance.isDestroyed()) {
                return;
            }
            FrameworkModel oldDefaultFrameworkModel = defaultInstance;
            //存在实例模型列表则直接从内存缓存中查后续不需要创建了
            if (allInstances.size() > 0) {
                defaultInstance = allInstances.get(0);
            } else {
                defaultInstance = null;
            }
            if (oldDefaultFrameworkModel != defaultInstance) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Reset global default framework from " + safeGetModelDesc(oldDefaultFrameworkModel)
                            + " to " + safeGetModelDesc(defaultInstance));
                }
            }
        }
    }

    private static String safeGetModelDesc(ScopeModel scopeModel) {
        return scopeModel != null ? scopeModel.getDesc() : null;
    }

    /**
     * Get all application models except for the internal application model.
     */
    public List<ApplicationModel> getApplicationModels() {
        synchronized (globalLock) {
            return Collections.unmodifiableList(pubApplicationModels);
        }
    }

    /**
     * Get all application models including the internal application model.
     */
    public List<ApplicationModel> getAllApplicationModels() {
        synchronized (globalLock) {
            return Collections.unmodifiableList(applicationModels);
        }
    }

    public ApplicationModel getInternalApplicationModel() {
        return internalApplicationModel;
    }

    public FrameworkServiceRepository getServiceRepository() {
        return serviceRepository;
    }

    @Override
    protected Lock acquireDestroyLock() {
        return destroyLock;
    }

    @Override
    public Environment modelEnvironment() {
        throw new UnsupportedOperationException("Environment is inaccessible for FrameworkModel");
    }

    @Override
    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return super.checkIfClassLoaderCanRemoved(classLoader)
                && applicationModels.stream()
                .noneMatch(applicationModel -> applicationModel.containsClassLoader(classLoader));
    }
}

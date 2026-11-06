package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.annotation.CacheLoad;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 启动期扫描 {@code @CacheLoad} 方法并注册元数据：
 *
 * <p>目的是让“值类型无法解析 / prefix 冲突 / key 为空”在应用启动时 fail-fast，
 * 同时把通用缓存前缀提前加入广播白名单（即使本实例尚未读取过该键，
 * 也能正确识别并清除其他实例发来的失效消息）。</p>
 *
 * <p>扫描目标类方法（含继承的泛型父类方法）与目标类实现的全部接口方法。
 * {@code postProcessBeforeInitialization} 阶段只记录候选类，不触发任何 bean 创建；
 * 待所有单例实例化完成后（{@link #afterSingletonsInstantiated()}）再统一扫描注册。
 * 若在 BPP 阶段直接获取 registry，会在 BeanPostProcessor 注册尚未完成时提前创建
 * {@link GenericCacheRegistry} 及其依赖，Spring 将输出
 * “Bean ... is not eligible for getting processed by all BeanPostProcessors” 警告，
 * 且这些 bean 会绕过之后注册的 BPP（自动代理等）。</p>
 *
 * <p>通用缓存关闭时 {@link GenericCacheRegistry} 不存在，扫描自动跳过。</p>
 */
public class GenericCacheLoadBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton {

    private final ObjectProvider<GenericCacheRegistry> registryProvider;
    private final Set<Class<?>> candidates = ConcurrentHashMap.newKeySet();

    public GenericCacheLoadBeanPostProcessor(ObjectProvider<GenericCacheRegistry> registryProvider) {
        this.registryProvider = registryProvider;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        // 仅记录目标类：此处获取 registry 会提前创建它并触发 BeanPostProcessorChecker 警告
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        if (targetClass != null) {
            candidates.add(targetClass);
        }
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        GenericCacheRegistry registry = registryProvider.getIfAvailable();
        if (registry == null) {
            return;
        }
        for (Class<?> targetClass : candidates) {
            scan(registry, targetClass);
        }
    }

    private static void scan(GenericCacheRegistry registry, Class<?> targetClass) {
        if (GenericCacheRegistry.class.isAssignableFrom(targetClass)) {
            return;
        }
        ReflectionUtils.doWithMethods(targetClass,
                method -> registerIfAnnotated(registry, method, targetClass));
        // @CacheLoad 允许标在接口方法上（实现类无需重复标注）；启动期同样注册，
        // 保证前缀白名单与 fail-fast 不依赖首次调用
        for (Class<?> iface : ClassUtils.getAllInterfacesForClassAsSet(targetClass)) {
            ReflectionUtils.doWithMethods(iface,
                    method -> registerIfAnnotated(registry, method, targetClass));
        }
    }

    private static void registerIfAnnotated(GenericCacheRegistry registry, Method method,
                                            Class<?> targetClass) {
        if (method.isBridge() || method.isSynthetic()
                || Modifier.isStatic(method.getModifiers())) {
            return;
        }
        CacheLoad annotation = AnnotatedElementUtils.findMergedAnnotation(method, CacheLoad.class);
        if (annotation != null) {
            registry.register(method, targetClass, annotation);
        }
    }
}

package io.github.biglv666.cachekit.warmup;

import io.github.biglv666.cachekit.annotation.CacheWarmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link CacheWarmup} 执行器：上下文就绪（全部单例实例化完成后）在后台守护线程中
 * 依次调用所有标注了 {@code @CacheWarmup} 的 Bean 方法，各方法异常隔离（仅告警）。
 *
 * <p>用 SmartLifecycle 而非 ApplicationReadyEvent：生命周期 start 在 refresh 完成后触发，
 * 与真实应用一致，且 ApplicationContextRunner 等测试环境同样生效。</p>
 */
public class CacheWarmupRunner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(CacheWarmupRunner.class);

    private final ApplicationContext context;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile Thread worker;

    public CacheWarmupRunner(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        worker = new Thread(this::runWarmup, "cache-kit-warmup");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void stop() {
        Thread w = worker;
        if (w != null) {
            // 预热是幂等的缓存回填：中断只是尽快放弃剩余预热，不影响正确性
            w.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return started.get() && worker != null && worker.isAlive();
    }

    private void runWarmup() {
        List<Runnable> tasks = collectWarmupTasks();
        if (tasks.isEmpty()) {
            return;
        }
        long t0 = System.nanoTime();
        int failed = 0;
        for (Runnable task : tasks) {
            try {
                task.run();
            } catch (Throwable t) {
                failed++;
                log.warn("cache-kit 预热方法执行失败（继续执行其余预热）", t);
            }
        }
        log.info("cache-kit 预热完成：{} 个方法，失败 {}，耗时 {}ms",
                tasks.size(), failed, (System.nanoTime() - t0) / 1_000_000);
    }

    /**
     * 扫描全部 Bean 定义中标注了 {@code @CacheWarmup} 的方法。
     * getType 不触发实例化；getBean 延迟到实际执行时（懒初始化 Bean 也会被预热触发实例化）。
     */
    private List<Runnable> collectWarmupTasks() {
        List<Runnable> tasks = new ArrayList<>();
        for (String name : context.getBeanDefinitionNames()) {
            Class<?> beanType;
            try {
                beanType = context.getType(name);
            } catch (Exception e) {
                continue;
            }
            if (beanType == null) {
                continue;
            }
            // CGLIB 代理类的父类即用户类：沿父类链扫描可命中代理 Bean 上的注解
            for (Class<?> c = beanType; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Method method : c.getDeclaredMethods()) {
                    if (AnnotationUtils.findAnnotation(method, CacheWarmup.class) != null) {
                        tasks.add(() -> invokeWarmup(name, method));
                    }
                }
            }
        }
        return tasks;
    }

    private void invokeWarmup(String beanName, Method method) {
        try {
            Object bean = context.getBean(beanName);
            Method target = AopUtils.selectInvocableMethod(method, bean.getClass());
            target.setAccessible(true);
            target.invoke(bean);
        } catch (ReflectiveOperationException e) {
            // 统一转为运行时异常：由外层逐方法隔离捕获并告警
            throw new IllegalStateException("预热方法调用失败: " + beanName + "." + method.getName(), e);
        }
    }
}

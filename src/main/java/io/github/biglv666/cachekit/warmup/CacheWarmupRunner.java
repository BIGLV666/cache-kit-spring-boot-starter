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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link CacheWarmup} 执行器：上下文就绪（全部单例实例化完成后）在后台守护线程中
 * 调用所有标注了 {@code @CacheWarmup} 的 Bean 方法，各方法异常隔离（仅告警）。
 *
 * <p>执行顺序按 {@link CacheWarmup#order()} 升序；并行度 1（默认）为纯顺序执行，
 * 大于 1 时同 order 的任务并发执行、不同 order 之间保持先后。
 * 用 SmartLifecycle 而非 ApplicationReadyEvent：生命周期 start 在 refresh 完成后触发，
 * 与真实应用一致，且 ApplicationContextRunner 等测试环境同样生效。</p>
 */
public class CacheWarmupRunner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(CacheWarmupRunner.class);

    private record Task(int order, String beanName, Method method, int seq) {
    }

    private final ApplicationContext context;
    private final int parallelism;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicInteger threadSeq = new AtomicInteger();
    private volatile Thread worker;

    public CacheWarmupRunner(ApplicationContext context) {
        this(context, 1);
    }

    public CacheWarmupRunner(ApplicationContext context, int parallelism) {
        this.context = context;
        this.parallelism = Math.max(1, parallelism);
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
        List<Task> tasks = collectWarmupTasks();
        if (tasks.isEmpty()) {
            return;
        }
        tasks.sort(Comparator.comparingInt(Task::order).thenComparingInt(Task::seq));
        long t0 = System.nanoTime();
        int failed = parallelism <= 1 ? runSequential(tasks) : runWaves(tasks);
        log.info("cache-kit 预热完成：{} 个方法，失败 {}，耗时 {}ms",
                tasks.size(), failed, (System.nanoTime() - t0) / 1_000_000);
    }

    private int runSequential(List<Task> tasks) {
        int failed = 0;
        for (Task task : tasks) {
            failed += runIsolated(task);
        }
        return failed;
    }

    /** 有序并发：order 相同的任务并行，不同 order 之间按波次保持先后 */
    private int runWaves(List<Task> tasks) {
        ExecutorService pool = Executors.newFixedThreadPool(parallelism, r -> {
            Thread t = new Thread(r, "cache-kit-warmup-" + threadSeq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        try {
            int failed = 0;
            int i = 0;
            while (i < tasks.size()) {
                int waveEnd = i;
                while (waveEnd < tasks.size() && tasks.get(waveEnd).order() == tasks.get(i).order()) {
                    waveEnd++;
                }
                List<Future<Integer>> futures = new ArrayList<>();
                for (Task task : tasks.subList(i, waveEnd)) {
                    futures.add(pool.submit(() -> runIsolated(task)));
                }
                for (Future<Integer> future : futures) {
                    try {
                        failed += future.get();
                    } catch (ExecutionException e) {
                        failed++;
                    }
                }
                i = waveEnd;
            }
            return failed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return tasks.size();
        } finally {
            pool.shutdownNow();
        }
    }

    private int runIsolated(Task task) {
        try {
            Object bean = context.getBean(task.beanName());
            Method target = AopUtils.selectInvocableMethod(task.method(), bean.getClass());
            target.setAccessible(true);
            target.invoke(bean);
            return 0;
        } catch (ReflectiveOperationException e) {
            log.warn("cache-kit 预热方法调用失败（继续执行其余预热）: {}.{}",
                    task.beanName(), task.method().getName(), e);
            return 1;
        } catch (Throwable t) {
            log.warn("cache-kit 预热方法执行失败（继续执行其余预热）: {}.{}",
                    task.beanName(), task.method().getName(), t);
            return 1;
        }
    }

    /**
     * 扫描全部 Bean 定义中标注了 {@code @CacheWarmup} 的方法。
     * getType 不触发实例化；getBean 延迟到实际执行时（懒初始化 Bean 也会被预热触发实例化）。
     * CGLIB 代理类的父类即用户类：沿父类链扫描可命中代理 Bean 上的注解。
     */
    private List<Task> collectWarmupTasks() {
        List<Task> tasks = new ArrayList<>();
        int seq = 0;
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
            for (Class<?> c = beanType; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Method method : c.getDeclaredMethods()) {
                    CacheWarmup annotation = AnnotationUtils.findAnnotation(method, CacheWarmup.class);
                    if (annotation != null) {
                        tasks.add(new Task(annotation.order(), name, method, seq++));
                    }
                }
            }
        }
        return tasks;
    }
}

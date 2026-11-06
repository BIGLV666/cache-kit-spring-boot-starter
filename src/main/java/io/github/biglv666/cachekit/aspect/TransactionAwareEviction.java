package io.github.biglv666.cachekit.aspect;

/**
 * 事务感知失效的统一入口：集中判断 spring-tx 是否存在、是否开启 afterCommit 策略，
 * 避免实体缓存切面与通用缓存句柄各维护一份分支而语义漂移。
 *
 * <p>本类不直接引用 spring-tx 类型；只有真实存在类路径时才调用
 * {@link TransactionAwareEvictor}，从而保持可选依赖语义。</p>
 */
public final class TransactionAwareEviction {

    private static final boolean SPRING_TX_PRESENT = detectSpringTx();

    private TransactionAwareEviction() {
    }

    /**
     * 按配置执行失效：活动事务内且开启 afterCommit 时延迟，否则立即执行。
     *
     * @param evictAction     失效动作
     * @param evictAfterCommit {@code cache-kit.tx.evict-after-commit}
     */
    public static void evictIfConfigured(Runnable evictAction, boolean evictAfterCommit) {
        if (evictAfterCommit && SPRING_TX_PRESENT) {
            TransactionAwareEvictor.evict(evictAction);
        } else {
            evictAction.run();
        }
    }

    private static boolean detectSpringTx() {
        try {
            Class.forName("org.springframework.transaction.support.TransactionSynchronizationManager");
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }
}

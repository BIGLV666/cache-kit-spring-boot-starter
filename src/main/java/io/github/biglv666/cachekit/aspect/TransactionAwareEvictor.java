package io.github.biglv666.cachekit.aspect;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 事务感知失效钩子（仅 spring-tx 在类路径且存在活动事务时被加载）：
 * 把缓存失效动作注册到事务提交后执行，消除"删除发生在事务提交前"的窗口——
 * 否则并发读会在删除后、提交前把旧值回填进缓存。
 *
 * <p>类由 {@link CacheInvalidateAspect} 按 spring-tx 存在性条件加载，非事务宿主不受影响。</p>
 */
final class TransactionAwareEvictor {

    private TransactionAwareEvictor() {
    }

    /**
     * @param evictAction 失效动作；真实活动事务内注册到 afterCommit，否则立即执行
     */
    static void evict(Runnable evictAction) {
        // 判据必须是"真实事务激活 + 同步器激活"两者兼备：仅有同步器而无真实事务时
        //（手工 initSynchronization、部分消息监听容器宿主）afterCommit 永远不会触发，
        // 注册上去的失效会被静默吞掉——此时必须立即执行
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()
                && !TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    evictAction.run();
                }
            });
        } else {
            evictAction.run();
        }
    }
}

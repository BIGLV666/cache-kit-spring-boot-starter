package io.github.biglv666.cachekit.core;

/**
 * 旁路上下文：激活后当前线程内的缓存读取直接透传到 loader（强一致读），
 * 失效操作不受影响。通过 {@link CacheKit#withDb} 使用，支持嵌套。
 */
public final class BypassContext {

    private static final ThreadLocal<Integer> DEPTH = ThreadLocal.withInitial(() -> 0);

    private BypassContext() {
    }

    /** 进入旁路作用域，返回是否为最外层进入（用于对称退出校验） */
    static boolean enter() {
        DEPTH.set(DEPTH.get() + 1);
        return DEPTH.get() == 1;
    }

    /** 退出旁路作用域 */
    static void exit() {
        int depth = DEPTH.get();
        if (depth <= 1) {
            DEPTH.remove();
        } else {
            DEPTH.set(depth - 1);
        }
    }

    /** 当前线程是否处于旁路作用域 */
    public static boolean isActive() {
        return DEPTH.get() > 0;
    }
}

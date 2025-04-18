package org.lupenghan.eazydb.backend.TransactionManager.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务时间戳生成器
 * 提供全局单调递增的时间戳
 */
public class TimestampGenerator {
    // 使用AtomicLong保证线程安全
    private static final AtomicLong TIMESTAMP = new AtomicLong(0);

    /**
     * 获取下一个时间戳
     * @return 时间戳
     */
    public static long nextTimestamp() {
        return TIMESTAMP.incrementAndGet();
    }

    /**
     * 获取当前最大时间戳（不增加）
     * @return 当前时间戳
     */
    public static long currentTimestamp() {
        return TIMESTAMP.get();
    }
}
package org.lupenghan.eazydb.backend.TransactionManager.utils;

import lombok.Getter;

/**
 * 事务隔离级别
 */
@Getter
public enum IsolationLevel {
    /**
     * 读未提交 - 允许脏读、不可重复读和幻读
     */
    READ_UNCOMMITTED(1),

    /**
     * 读已提交 - 防止脏读，但允许不可重复读和幻读
     */
    READ_COMMITTED(2),

    /**
     * 可重复读 - 防止脏读和不可重复读，但允许幻读
     */
    REPEATABLE_READ(3),

    /**
     * 串行化 - 最高隔离级别，防止所有并发问题
     */
    SERIALIZABLE(4);

    private final int value;

    IsolationLevel(int value) {
        this.value = value;
    }

    /**
     * 根据整数值获取隔离级别
     * @param value 整数值
     * @return 隔离级别
     */
    public static IsolationLevel fromValue(int value) {
        for (IsolationLevel level : values()) {
            if (level.value == value) {
                return level;
            }
        }
        return REPEATABLE_READ; // 默认使用可重复读
    }
}
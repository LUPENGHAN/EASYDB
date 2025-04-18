package org.lupenghan.eazydb.backend.TransactionManager.utils;

import lombok.Getter;

/**
 * 死锁异常
 */
@Getter
public class DeadlockException extends Exception {

    private final long victimXID;

    /**
     * 创建死锁异常
     * @param message 异常消息
     * @param victimXID 死锁受害者事务ID
     */
    public DeadlockException(String message, long victimXID) {
        super(message);
        this.victimXID = victimXID;
    }

}
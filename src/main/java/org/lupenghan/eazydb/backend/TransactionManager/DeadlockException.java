package org.lupenghan.eazydb.backend.TransactionManager;

/**
 * 死锁异常
 */
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

    /**
     * 获取死锁受害者事务ID
     * @return 事务ID
     */
    public long getVictimXID() {
        return victimXID;
    }
}
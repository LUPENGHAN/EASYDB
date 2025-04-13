package org.lupenghan.eazydb.backend.DataManager;


import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManagerImpl;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManagerImpl;

public class DBSystem {
    private TransactionManager tm;
    private LogManager lm;
    private String path;

    public DBSystem(String path) throws Exception {
        this.path = path;
        this.tm = new TransactionManagerImpl(path);
        this.lm = new LogManagerImpl(path);

        // 系统启动时执行恢复
        recover();
    }

    private void recover() {
        // 执行数据库恢复
        lm.recover();
    }

    // 开始一个新事务
    public long begin() {
        return tm.begin();
    }

    // 提交事务
    public void commit(long xid) {
        // 1. 记录提交日志
        // 2. 执行实际的提交
        tm.commit(xid);
    }

    // 中止事务
    public void abort(long xid) {
        // 1. 记录中止日志
        // 2. 执行实际的中止
        tm.abort(xid);
    }

    // 关闭数据库系统
    public void close() {
        // 先创建检查点
        lm.checkpoint();

        // 关闭各组件
        lm.close();
        tm.close();
    }

    // 其他与数据操作相关的方法
}
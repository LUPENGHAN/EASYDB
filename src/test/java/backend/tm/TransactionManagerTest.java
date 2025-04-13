package backend.tm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lupenghan.eazydb.backend.tm.TransactionManager;
import org.lupenghan.eazydb.backend.tm.TransactionManagerImpl;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TransactionManagerTest {
    private TransactionManager tm;
    private String testDir = "testdata/txn_test";

    @Before
    public void setUp() throws Exception {
        // 清理旧测试数据
        File dir = new File(testDir);
        if(dir.exists()) {
            for(File file : dir.listFiles()) {
                file.delete();
            }
            dir.delete();
        }

        // 创建新的事务管理器
        tm = new TransactionManagerImpl(testDir);
    }

    @Test
    public void testBeginCommit() {
        long xid = tm.begin();
        assertTrue(tm.isActive(xid));
        assertFalse(tm.isCommitted(xid));
        assertFalse(tm.isAbort(xid));

        tm.commit(xid);
        assertFalse(tm.isActive(xid));
        assertTrue(tm.isCommitted(xid));
        assertFalse(tm.isAbort(xid));
    }

    @Test
    public void testBeginAbort() {
        long xid = tm.begin();
        assertTrue(tm.isActive(xid));

        tm.abort(xid);
        assertFalse(tm.isActive(xid));
        assertFalse(tm.isCommitted(xid));
        assertTrue(tm.isAbort(xid));
    }

    @Test
    public void testRecover() throws Exception {
        // 创建并提交事务
        long xid1 = tm.begin();
        long xid2 = tm.begin();
        tm.commit(xid1);
        tm.abort(xid2);

        // 关闭并重新打开
        tm.close();
        tm = new TransactionManagerImpl(testDir);

        // 检查恢复后的状态
        assertTrue(tm.isCommitted(xid1));
        assertTrue(tm.isAbort(xid2));

        // 新事务应该可以正常工作
        long xid3 = tm.begin();
        assertTrue(tm.isActive(xid3));
    }

    @After
    public void tearDown() {
        tm.close();
    }
}
package backend.tm;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.Record;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl.RecordManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl.TablespaceManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.TablespaceManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.BufferPoolManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.DiskManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Impl.BufferPoolManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Impl.DiskManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Impl.PageManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManagerImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Scanner;
import java.util.function.Predicate;

/**
 * 数据条目管理器测试类
 */
public class DataEntryManagerTest {

    // 存储路径
    private static final String DB_PATH = "./eazydb_data";

    // 缓冲池大小
    private static final int BUFFER_POOL_SIZE = 1024;

    // 测试表ID
    private static final int TEST_TABLE_ID = 1;

    // 组件
    private TransactionManager txManager;
    private LogManager logManager;
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private PageManager pageManager;
    private TablespaceManager tablespaceManager;
    private RecordManager recordManager;

    /**
     * 初始化所有组件
     */
    public void init() throws Exception {
        // 初始化事务管理器
        txManager = new TransactionManagerImpl(DB_PATH);

        // 初始化日志管理器
        logManager = new LogManagerImpl(DB_PATH);

        // 初始化磁盘管理器
        diskManager = new DiskManagerImpl(DB_PATH);

        // 初始化缓冲池管理器
        bufferPoolManager = new BufferPoolManagerImpl(BUFFER_POOL_SIZE, diskManager);

        // 初始化页面管理器
        pageManager = new PageManagerImpl(bufferPoolManager, logManager);

        // 初始化表空间管理器
        tablespaceManager = new TablespaceManagerImpl(pageManager, DB_PATH);

        // 初始化记录管理器
        recordManager = new RecordManagerImpl(pageManager, txManager, logManager, tablespaceManager, TEST_TABLE_ID);
    }

    /**
     * 关闭所有组件
     */
    public void close() {
        // 关闭记录管理器
        // 关闭表空间管理器（如果需要的话）
        if (tablespaceManager instanceof TablespaceManagerImpl) {
            ((TablespaceManagerImpl) tablespaceManager).close();
        }

        // 关闭页面管理器
        pageManager.close();

        // 关闭日志管理器
        logManager.close();

        // 关闭事务管理器
        txManager.close();

        // 关闭磁盘管理器
        try {
            diskManager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 运行基本测试
     */
    public void runBasicTest() throws Exception {
        System.out.println("======= 开始基本数据条目测试 =======");

        // 开始一个事务
        long xid = txManager.begin();
        System.out.println("创建新事务: XID = " + xid);

        try {
            // 插入一条记录
            String data1 = "这是第一条测试记录";
            byte[] record1 = data1.getBytes(StandardCharsets.UTF_8);
            RecordID rid1 = recordManager.insertRecord(record1, xid);
            System.out.println("插入记录1，RID = " + rid1);

            // 插入另一条记录
            String data2 = "这是第二条测试记录，内容较长一些";
            byte[] record2 = data2.getBytes(StandardCharsets.UTF_8);
            RecordID rid2 = recordManager.insertRecord(record2, xid);
            System.out.println("插入记录2，RID = " + rid2);

            // 读取第一条记录
            byte[] readRecord1 = recordManager.getRecord(rid1, xid);
            String readData1 = new String(readRecord1, StandardCharsets.UTF_8);
            System.out.println("读取记录1: " + readData1);

            // 读取第二条记录
            byte[] readRecord2 = recordManager.getRecord(rid2, xid);
            String readData2 = new String(readRecord2, StandardCharsets.UTF_8);
            System.out.println("读取记录2: " + readData2);

            // 更新第一条记录
            String updatedData = "这是更新后的第一条记录";
            byte[] updatedRecord = updatedData.getBytes(StandardCharsets.UTF_8);
            boolean updateResult = recordManager.updateRecord(rid1, updatedRecord, xid);
            System.out.println("更新记录1: " + (updateResult ? "成功" : "失败"));

            // 再次读取第一条记录
            byte[] readUpdatedRecord = recordManager.getRecord(rid1, xid);
            String readUpdatedData = new String(readUpdatedRecord, StandardCharsets.UTF_8);
            System.out.println("读取更新后的记录1: " + readUpdatedData);

            // 删除第二条记录
            boolean deleteResult = recordManager.deleteRecord(rid2, xid);
            System.out.println("删除记录2: " + (deleteResult ? "成功" : "失败"));

            // 尝试读取已删除记录
            byte[] readDeletedRecord = recordManager.getRecord(rid2, xid);
            if (readDeletedRecord == null) {
                System.out.println("已删除记录2无法读取");
            } else {
                System.out.println("读取已删除记录2: " + new String(readDeletedRecord, StandardCharsets.UTF_8));
            }

            // 提交事务
            txManager.commit(xid);
            System.out.println("事务提交: XID = " + xid);

            // 验证提交后数据是否仍然可见
            long newXid = txManager.begin();
            System.out.println("创建新事务: XID = " + newXid);

            byte[] verifyRecord = recordManager.getRecord(rid1, newXid);
            String verifyData = new String(verifyRecord, StandardCharsets.UTF_8);
            System.out.println("提交后验证记录1: " + verifyData);

            // 尝试验证已删除的记录2
            byte[] verifyDeletedRecord = recordManager.getRecord(rid2, newXid);
            if (verifyDeletedRecord == null) {
                System.out.println("提交后已删除记录2无法读取");
            } else {
                System.out.println("提交后读取已删除记录2: " + new String(verifyDeletedRecord, StandardCharsets.UTF_8));
            }

            // 提交新事务
            txManager.commit(newXid);
            System.out.println("新事务提交: XID = " + newXid);

        } catch (Exception e) {
            // 发生异常，回滚事务
            txManager.abort(xid);
            System.out.println("事务回滚: XID = " + xid);
            throw e;
        }

        System.out.println("======= 基本数据条目测试完成 =======");
    }

    /**
     * 运行批量插入测试
     */
    public void runBulkInsertTest() throws Exception {
        System.out.println("======= 开始批量插入测试 =======");

        // 开始一个事务
        long xid = txManager.begin();
        System.out.println("创建新事务: XID = " + xid);

        try {
            // 批量插入记录
            int recordCount = 100;
            RecordID[] recordIDs = new RecordID[recordCount];

            for (int i = 0; i < recordCount; i++) {
                String data = "批量插入测试记录 #" + (i + 1);
                byte[] record = data.getBytes(StandardCharsets.UTF_8);
                recordIDs[i] = recordManager.insertRecord(record, xid);
                if ((i + 1) % 10 == 0) {
                    System.out.println("已插入 " + (i + 1) + " 条记录");
                }
            }

            // 随机读取部分记录
            System.out.println("随机验证一些记录:");
            for (int i = 0; i < 5; i++) {
                int index = (int) (Math.random() * recordCount);
                byte[] readRecord = recordManager.getRecord(recordIDs[index], xid);
                String readData = new String(readRecord, StandardCharsets.UTF_8);
                System.out.println("读取记录 #" + (index + 1) + ": " + readData);
            }

            // 提交事务
            txManager.commit(xid);
            System.out.println("事务提交: XID = " + xid);

        } catch (Exception e) {
            // 发生异常，回滚事务
            txManager.abort(xid);
            System.out.println("事务回滚: XID = " + xid);
            throw e;
        }

        System.out.println("======= 批量插入测试完成 =======");
    }

    /**
     * 运行事务隔离测试
     */
    public void runTransactionIsolationTest() throws Exception {
        System.out.println("======= 开始事务隔离测试 =======");

        // 事务1: 插入记录
        long xid1 = txManager.begin();
        System.out.println("创建事务1: XID = " + xid1);

        String data = "事务隔离测试记录";
        byte[] record = data.getBytes(StandardCharsets.UTF_8);
        RecordID rid = recordManager.insertRecord(record, xid1);
        System.out.println("事务1插入记录: RID = " + rid);

        // 事务2: 尝试读取事务1未提交的记录
        long xid2 = txManager.begin();
        System.out.println("创建事务2: XID = " + xid2);

        byte[] readRecord = recordManager.getRecord(rid, xid2);
        if (readRecord == null) {
            System.out.println("事务2无法读取事务1未提交的记录 (符合预期)");
        } else {
            System.out.println("事务2读取到事务1未提交的记录: " + new String(readRecord, StandardCharsets.UTF_8) + " (隔离性问题)");
        }

        // 事务1提交
        txManager.commit(xid1);
        System.out.println("事务1提交");

        // 事务2再次尝试读取
        readRecord = recordManager.getRecord(rid, xid2);
        if (readRecord == null) {
            System.out.println("事务2无法读取事务1已提交的记录 (符合快照隔离预期)");
        } else {
            String readData = new String(readRecord, StandardCharsets.UTF_8);
            System.out.println("事务2读取到事务1已提交的记录: " + readData + " (符合读已提交预期)");
        }

        // 事务2提交
        txManager.commit(xid2);
        System.out.println("事务2提交");

        // 新事务3验证
        long xid3 = txManager.begin();
        System.out.println("创建事务3: XID = " + xid3);

        readRecord = recordManager.getRecord(rid, xid3);
        if (readRecord != null) {
            String readData = new String(readRecord, StandardCharsets.UTF_8);
            System.out.println("事务3读取已提交记录: " + readData);
        } else {
            System.out.println("事务3无法读取记录 (数据丢失问题)");
        }

        // 事务3提交
        txManager.commit(xid3);
        System.out.println("事务3提交");

        System.out.println("======= 事务隔离测试完成 =======");
    }

    /**
     * 运行简单交互式测试
     */
    public void runInteractiveTest() throws Exception {
        Scanner scanner = new Scanner(System.in);
        long xid = 0;
        boolean inTransaction = false;

        System.out.println("======= 开始交互式测试 =======");
        System.out.println("输入命令: begin, commit, abort, insert, read, update, delete, list, exit");

        while (true) {
            System.out.print("\n> ");
            String command = scanner.nextLine().trim();

            try {
                if (command.equals("exit")) {
                    break;
                } else if (command.equals("begin")) {
                    if (inTransaction) {
                        System.out.println("已有活动事务，请先提交或中止");
                    } else {
                        xid = txManager.begin();
                        inTransaction = true;
                        System.out.println("事务开始: XID = " + xid);
                    }
                } else if (command.equals("commit")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        txManager.commit(xid);
                        inTransaction = false;
                        System.out.println("事务提交: XID = " + xid);
                    }
                } else if (command.equals("abort")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        txManager.abort(xid);
                        inTransaction = false;
                        System.out.println("事务中止: XID = " + xid);
                    }
                } else if (command.equals("insert")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        System.out.print("输入记录内容: ");
                        String data = scanner.nextLine();
                        byte[] record = data.getBytes(StandardCharsets.UTF_8);
                        RecordID rid = recordManager.insertRecord(record, xid);
                        System.out.println("记录已插入: RID = " + rid);
                    }
                } else if (command.startsWith("read")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        System.out.print("输入记录ID (fileID pageNum slotNum): ");
                        String ridStr = scanner.nextLine();
                        String[] parts = ridStr.split("\\s+");
                        if (parts.length == 3) {
                            int fileID = Integer.parseInt(parts[0]);
                            int pageNum = Integer.parseInt(parts[1]);
                            int slotNum = Integer.parseInt(parts[2]);
                            RecordID rid = new RecordID(new PageID(fileID, pageNum), slotNum);
                            byte[] readRecord = recordManager.getRecord(rid, xid);
                            if (readRecord != null) {
                                System.out.println("读取记录: " + new String(readRecord, StandardCharsets.UTF_8));
                            } else {
                                System.out.println("记录不存在或不可见");
                            }
                        } else {
                            System.out.println("无效的记录ID格式");
                        }
                    }
                } else if (command.startsWith("update")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        System.out.print("输入记录ID (fileID pageNum slotNum): ");
                        String ridStr = scanner.nextLine();
                        String[] parts = ridStr.split("\\s+");
                        if (parts.length == 3) {
                            int fileID = Integer.parseInt(parts[0]);
                            int pageNum = Integer.parseInt(parts[1]);
                            int slotNum = Integer.parseInt(parts[2]);
                            RecordID rid = new RecordID(new PageID(fileID, pageNum), slotNum);

                            System.out.print("输入新记录内容: ");
                            String data = scanner.nextLine();
                            byte[] record = data.getBytes(StandardCharsets.UTF_8);

                            boolean result = recordManager.updateRecord(rid, record, xid);
                            if (result) {
                                System.out.println("记录已更新");
                            } else {
                                System.out.println("更新失败，记录可能不存在或不可见");
                            }
                        } else {
                            System.out.println("无效的记录ID格式");
                        }
                    }
                } else if (command.startsWith("delete")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        System.out.print("输入记录ID (fileID pageNum slotNum): ");
                        String ridStr = scanner.nextLine();
                        String[] parts = ridStr.split("\\s+");
                        if (parts.length == 3) {
                            int fileID = Integer.parseInt(parts[0]);
                            int pageNum = Integer.parseInt(parts[1]);
                            int slotNum = Integer.parseInt(parts[2]);
                            RecordID rid = new RecordID(new PageID(fileID, pageNum), slotNum);

                            boolean result = recordManager.deleteRecord(rid, xid);
                            if (result) {
                                System.out.println("记录已删除");
                            } else {
                                System.out.println("删除失败，记录可能不存在或不可见");
                            }
                        } else {
                            System.out.println("无效的记录ID格式");
                        }
                    }
                }         else if (command.equals("list")) {
                    if (!inTransaction) {
                        System.out.println("没有活动事务，请先开始事务");
                    } else {
                        System.out.println("列出所有记录 (最多显示10条):");
                        // 使用null作为Predicate<Record>参数
                        Iterator<Record> records = recordManager.scanRecords((Predicate<Record>)null, xid);
                        int count = 0;
                        while (records.hasNext() && count < 10) {
                            Record record = records.next();
                            String data = new String(record.getData(), StandardCharsets.UTF_8);
                            System.out.println(record.getRid() + ": " + data);
                            count++;
                        }
                        if (!records.hasNext()) {
                            System.out.println("共显示 " + count + " 条记录");
                        } else {
                            System.out.println("仅显示前 10 条记录，还有更多...");
                        }
                    }
                } else {
                    System.out.println("未知命令: " + command);
                }
            } catch (Exception e) {
                System.out.println("操作失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // 如果事务仍在进行，尝试中止
        if (inTransaction) {
            try {
                txManager.abort(xid);
                System.out.println("退出前中止未完成事务: XID = " + xid);
            } catch (Exception e) {
                System.out.println("中止事务失败: " + e.getMessage());
            }
        }

        System.out.println("======= 交互式测试结束 =======");
    }

    public static void main(String[] args) {
        DataEntryManagerTest test = new DataEntryManagerTest();

        try {
            // 初始化
            test.init();

            // 运行测试
            test.runBasicTest();
            test.runBulkInsertTest();
            test.runTransactionIsolationTest();

            // 交互式测试
            test.runInteractiveTest();

        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭系统
            test.close();
        }
    }
}
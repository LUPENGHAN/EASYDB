package org.lupenghan.eazydb.backend.DataManager.PageManager.Impl;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.DiskManager;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 磁盘管理器实现类
 */
public class DiskManagerImpl implements DiskManager {
    // 最大文件数
    private static final int MAX_FILES = 100;

    // 文件名到文件ID的映射
    private final Map<String, Integer> fileNameToIdMap;

    // 文件ID到通道的映射
    private final Map<Integer, FileChannel> fileChannels;

    // 文件ID到文件名的映射
    private final Map<Integer, String> fileIdToNameMap;

    // 空闲页面管理（按文件ID分组）
    private final Map<Integer, BitSet> freePageMap;

    // 下一个可用的文件ID
    private final AtomicInteger nextFileId;

    // 每个文件的页数（按文件ID分组）
    private final Map<Integer, Integer> filePageCount;

    // 数据库目录路径
    private final String dbDirectory;

    // 用于并发控制的锁
    private final ReadWriteLock[] fileLocks;

    /**
     * 创建一个磁盘管理器实例
     * @param dbDirectory 数据库文件目录
     * @throws IOException 如果目录创建失败
     */
    public DiskManagerImpl(String dbDirectory) throws IOException {
        this.dbDirectory = dbDirectory;
        this.fileNameToIdMap = new ConcurrentHashMap<>();
        this.fileChannels = new ConcurrentHashMap<>();
        this.fileIdToNameMap = new ConcurrentHashMap<>();
        this.freePageMap = new ConcurrentHashMap<>();
        this.filePageCount = new ConcurrentHashMap<>();
        this.nextFileId = new AtomicInteger(0);
        this.fileLocks = new ReentrantReadWriteLock[MAX_FILES];

        // 初始化文件锁
        for (int i = 0; i < MAX_FILES; i++) {
            fileLocks[i] = new ReentrantReadWriteLock();
        }

        // 确保数据库目录存在
        File dir = new File(dbDirectory);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("无法创建数据库目录: " + dbDirectory);
            }
        }

        // 加载现有的数据库文件
        loadExistingFiles();
    }

    /**
     * 加载现有的数据库文件
     * @throws IOException 如果文件加载失败
     */
    private void loadExistingFiles() throws IOException {
        File dir = new File(dbDirectory);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".db"));

        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                // 移除.db后缀
                String baseName = fileName.substring(0, fileName.length() - 3);

                // 打开文件
                int fileId = openFile(baseName);

                // 计算文件中的页数
                long fileSize = file.length();
                int pageCount = (int) (fileSize / PageImpl.PAGE_SIZE);
                filePageCount.put(fileId, pageCount);

                // 初始化空闲页面位图
                BitSet freePages = new BitSet(pageCount);
                // 默认所有页面都是已使用的
                freePages.set(0, pageCount);

                // TODO: 加载空闲页面信息，可能需要从元数据文件中读取

                freePageMap.put(fileId, freePages);
            }
        }
    }

    @Override
    public byte[] readPage(PageID pageID) throws IOException {
        int fileId = pageID.getFileID();
        int pageNum = pageID.getPageNum();

        // 获取读锁
        fileLocks[fileId].readLock().lock();
        try {
            FileChannel channel = getFileChannel(fileId);
            if (channel == null) {
                return null;
            }

            // 检查页码是否有效
            if (pageNum >= filePageCount.getOrDefault(fileId, 0)) {
                return null;
            }

            // 计算页面偏移量
            long offset = (long) pageNum * PageImpl.PAGE_SIZE;

            // 分配缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(PageImpl.PAGE_SIZE);

            // 读取页面
            channel.read(buffer, offset);
            buffer.flip();

            return buffer.array();
        } finally {
            fileLocks[fileId].readLock().unlock();
        }
    }

    @Override
    public void writePage(PageID pageID, byte[] data) throws IOException {
        if (data.length != PageImpl.PAGE_SIZE) {
            throw new IllegalArgumentException("页面数据大小必须为" + PageImpl.PAGE_SIZE + "字节");
        }

        int fileId = pageID.getFileID();
        int pageNum = pageID.getPageNum();

        // 获取写锁
        fileLocks[fileId].writeLock().lock();
        try {
            FileChannel channel = getFileChannel(fileId);
            if (channel == null) {
                throw new IOException("文件未打开: fileId=" + fileId);
            }

            // 计算页面偏移量
            long offset = (long) pageNum * PageImpl.PAGE_SIZE;

            // 写入页面
            ByteBuffer buffer = ByteBuffer.wrap(data);
            channel.write(buffer, offset);

            // 更新文件页数（如果需要）
            int currentPageCount = filePageCount.getOrDefault(fileId, 0);
            if (pageNum >= currentPageCount) {
                filePageCount.put(fileId, pageNum + 1);
            }
        } finally {
            fileLocks[fileId].writeLock().unlock();
        }
    }

    @Override
    public PageID allocatePage() throws IOException {
        // 尝试在现有文件中找到一个空闲页面
        for (Map.Entry<Integer, BitSet> entry : freePageMap.entrySet()) {
            int fileId = entry.getKey();
            BitSet freePages = entry.getValue();

            fileLocks[fileId].writeLock().lock();
            try {
                // 查找第一个空闲页面
                int freePageNum = freePages.nextClearBit(0);

                // 如果找到了空闲页面，标记为已使用并返回
                if (freePageNum < freePages.size()) {
                    freePages.set(freePageNum);
                    return new PageID(fileId, freePageNum);
                }

                // 如果没有找到空闲页面，在文件末尾添加新页面
                int pageCount = filePageCount.getOrDefault(fileId, 0);
                PageID pageID = new PageID(fileId, pageCount);

                // 创建空页面
                byte[] emptyPage = new byte[PageImpl.PAGE_SIZE];
                writePage(pageID, emptyPage);

                // 更新空闲页面位图
                freePages.set(pageCount);
                filePageCount.put(fileId, pageCount + 1);

                return pageID;
            } finally {
                fileLocks[fileId].writeLock().unlock();
            }
        }

        // 如果没有现有文件或所有文件都已满，创建新文件
        String newFileName = "data_" + System.currentTimeMillis();
        int fileId = createFile(newFileName);

        // 使用新文件的第一个页面
        PageID pageID = new PageID(fileId, 0);

        // 创建空页面
        byte[] emptyPage = new byte[PageImpl.PAGE_SIZE];
        writePage(pageID, emptyPage);

        // 更新空闲页面位图
        BitSet freePages = new BitSet();
        freePages.set(0);
        freePageMap.put(fileId, freePages);
        filePageCount.put(fileId, 1);

        return pageID;
    }

    @Override
    public boolean deallocatePage(PageID pageID) throws IOException {
        int fileId = pageID.getFileID();
        int pageNum = pageID.getPageNum();

        fileLocks[fileId].writeLock().lock();
        try {
            BitSet freePages = freePageMap.get(fileId);
            if (freePages == null) {
                return false;
            }

            // 检查页码是否有效
            if (pageNum >= filePageCount.getOrDefault(fileId, 0)) {
                return false;
            }

            // 检查页面是否已经是空闲的
            if (!freePages.get(pageNum)) {
                return false;
            }

            // 标记页面为空闲
            freePages.clear(pageNum);

            return true;
        } finally {
            fileLocks[fileId].writeLock().unlock();
        }
    }

    @Override
    public int getFilePageCount(int fileID) throws IOException {
        fileLocks[fileID].readLock().lock();
        try {
            return filePageCount.getOrDefault(fileID, 0);
        } finally {
            fileLocks[fileID].readLock().unlock();
        }
    }

    @Override
    public int createFile(String fileName) throws IOException {
        // 防止文件名重复
        if (fileNameToIdMap.containsKey(fileName)) {
            throw new IOException("文件已存在: " + fileName);
        }

        // 获取新的文件ID
        int fileId = nextFileId.getAndIncrement();

        // 构建完整的文件路径
        String filePath = dbDirectory + File.separator + fileName + ".db";

        // 创建文件
        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        FileChannel channel = file.getChannel();

        // 更新映射
        fileChannels.put(fileId, channel);
        fileNameToIdMap.put(fileName, fileId);
        fileIdToNameMap.put(fileId, fileName);

        // 初始化空闲页面位图
        freePageMap.put(fileId, new BitSet());
        filePageCount.put(fileId, 0);

        return fileId;
    }

    @Override
    public int openFile(String fileName) throws IOException {
        // 如果文件已经打开，直接返回文件ID
        if (fileNameToIdMap.containsKey(fileName)) {
            return fileNameToIdMap.get(fileName);
        }

        // 构建完整的文件路径
        String filePath = dbDirectory + File.separator + fileName + ".db";

        // 检查文件是否存在
        if (!Files.exists(Paths.get(filePath))) {
            // 文件不存在，创建新文件
            return createFile(fileName);
        }

        // 获取新的文件ID
        int fileId = nextFileId.getAndIncrement();

        // 打开文件
        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        FileChannel channel = file.getChannel();

        // 更新映射
        fileChannels.put(fileId, channel);
        fileNameToIdMap.put(fileName, fileId);
        fileIdToNameMap.put(fileId, fileName);

        // 计算文件中的页数
        long fileSize = file.length();
        int pageCount = (int) (fileSize / PageImpl.PAGE_SIZE);
        filePageCount.put(fileId, pageCount);

        // 初始化空闲页面位图
        BitSet freePages = new BitSet(pageCount);
        // 默认所有页面都是已使用的
        freePages.set(0, pageCount);
        freePageMap.put(fileId, freePages);

        return fileId;
    }

    @Override
    public void closeFile(int fileID) throws IOException {
        fileLocks[fileID].writeLock().lock();
        try {
            FileChannel channel = fileChannels.remove(fileID);
            if (channel != null) {
                channel.close();
            }

            String fileName = fileIdToNameMap.remove(fileID);
            if (fileName != null) {
                fileNameToIdMap.remove(fileName);
            }

            freePageMap.remove(fileID);
            filePageCount.remove(fileID);
        } finally {
            fileLocks[fileID].writeLock().unlock();
        }
    }

    @Override
    public boolean deleteFile(String fileName) throws IOException {
        // 如果文件已打开，先关闭它
        if (fileNameToIdMap.containsKey(fileName)) {
            int fileId = fileNameToIdMap.get(fileName);
            closeFile(fileId);
        }

        // 构建完整的文件路径
        String filePath = dbDirectory + File.separator + fileName + ".db";

        // 删除文件
        return Files.deleteIfExists(Paths.get(filePath));
    }

    @Override
    public void flushAll() throws IOException {
        for (Map.Entry<Integer, FileChannel> entry : fileChannels.entrySet()) {
            int fileId = entry.getKey();
            FileChannel channel = entry.getValue();

            fileLocks[fileId].readLock().lock();
            try {
                channel.force(true);
            } finally {
                fileLocks[fileId].readLock().unlock();
            }
        }
    }

    @Override
    public void close() throws IOException {
        // 保存空闲页面信息到元数据文件
        saveMetadata();

        // 关闭所有文件
        for (int fileId : new ArrayList<>(fileChannels.keySet())) {
            closeFile(fileId);
        }
    }

    /**
     * 获取文件通道
     * @param fileId 文件ID
     * @return 文件通道，如果文件未打开则返回null
     */
    private FileChannel getFileChannel(int fileId) {
        return fileChannels.get(fileId);
    }

    /**
     * 保存元数据到文件
     * @throws IOException 如果保存失败
     */
    private void saveMetadata() throws IOException {
        // 创建元数据文件
        String metadataPath = dbDirectory + File.separator + "metadata.dat";
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataPath))) {
            // 保存文件名到ID的映射
            oos.writeObject(new HashMap<>(fileNameToIdMap));

            // 保存文件ID到文件名的映射
            oos.writeObject(new HashMap<>(fileIdToNameMap));

            // 保存空闲页面信息
            Map<Integer, byte[]> serializedFreePages = new HashMap<>();
            for (Map.Entry<Integer, BitSet> entry : freePageMap.entrySet()) {
                serializedFreePages.put(entry.getKey(), entry.getValue().toByteArray());
            }
            oos.writeObject(serializedFreePages);

            // 保存文件页数信息
            oos.writeObject(new HashMap<>(filePageCount));

            // 保存下一个可用的文件ID
            oos.writeInt(nextFileId.get());
        }
    }

    /**
     * 从文件加载元数据
     * @throws IOException 如果加载失败
     * @throws ClassNotFoundException 如果反序列化失败
     */
    @SuppressWarnings("unchecked")
    private void loadMetadata() throws IOException, ClassNotFoundException {
        String metadataPath = dbDirectory + File.separator + "metadata.dat";
        File metadataFile = new File(metadataPath);

        if (!metadataFile.exists()) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(metadataPath))) {
            // 加载文件名到ID的映射
            Map<String, Integer> loadedFileNameToIdMap = (Map<String, Integer>) ois.readObject();
            fileNameToIdMap.putAll(loadedFileNameToIdMap);

            // 加载文件ID到文件名的映射
            Map<Integer, String> loadedFileIdToNameMap = (Map<Integer, String>) ois.readObject();
            fileIdToNameMap.putAll(loadedFileIdToNameMap);

            // 加载空闲页面信息
            Map<Integer, byte[]> serializedFreePages = (Map<Integer, byte[]>) ois.readObject();
            for (Map.Entry<Integer, byte[]> entry : serializedFreePages.entrySet()) {
                BitSet bitSet = BitSet.valueOf(entry.getValue());
                freePageMap.put(entry.getKey(), bitSet);
            }

            // 加载文件页数信息
            Map<Integer, Integer> loadedFilePageCount = (Map<Integer, Integer>) ois.readObject();
            filePageCount.putAll(loadedFilePageCount);

            // 加载下一个可用的文件ID
            int nextId = ois.readInt();
            nextFileId.set(nextId);
        }
    }
}
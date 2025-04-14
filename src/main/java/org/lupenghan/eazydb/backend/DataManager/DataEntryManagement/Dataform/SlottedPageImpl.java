package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordPage;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageHeader;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 槽式页面实现类，用于管理记录的存储
 */
public class SlottedPageImpl implements RecordPage {
    // 页面常量
    private static final int SLOT_COUNT_OFFSET = PageHeader.PAGE_HEADER_SIZE; // 槽数量偏移量
    private static final int FREE_SPACE_OFFSET = SLOT_COUNT_OFFSET + 2;       // 空闲空间偏移量偏移
    private static final int SLOT_DIRECTORY_OFFSET = FREE_SPACE_OFFSET + 2;   // 槽目录起始偏移
    private static final int SLOT_SIZE = 4;                                   // 每个槽占用4字节

    // 槽内容格式: [offset:2字节][length:2字节]
    private static final int SLOT_OFFSET_SIZE = 2;
    private static final int SLOT_LENGTH_SIZE = 2;

    // 槽状态标志
    private static final short SLOT_EMPTY = -1;

    // 页面结构
    private Page page;                // 底层存储页面

    /**
     * 创建一个槽式页面
     * @param page 底层页面
     */
    public SlottedPageImpl(Page page) {
        this.page = page;

        // 检查页面是否已初始化
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        short slotCount = buffer.getShort(SLOT_COUNT_OFFSET);

        if (slotCount == 0) {
            // 页面未初始化，设置初始值
            initializePage();
        }
    }

    /**
     * 初始化页面，设置槽数量为0，空闲空间指针为页面末尾
     */
    private void initializePage() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putShort((short) 0); // 槽数量初始化为0
        buffer.putShort((short) page.getData().length); // 空闲空间指针初始化为页面末尾
        buffer.flip();

        page.writeData(SLOT_COUNT_OFFSET, buffer.array());
        page.setDirty(true);
    }

    /**
     * 获取页面中的槽数量
     * @return 槽数量
     */
    private short getSlotCount() {
        byte[] data = page.readData(SLOT_COUNT_OFFSET, 2);
        return ByteBuffer.wrap(data).getShort();
    }

    /**
     * 获取空闲空间指针（页面数据区起始位置）
     * @return 空闲空间指针
     */
    private short getFreeSpacePointer() {
        byte[] data = page.readData(FREE_SPACE_OFFSET, 2);
        return ByteBuffer.wrap(data).getShort();
    }

    /**
     * 更新槽数量
     * @param count 新的槽数量
     */
    private void updateSlotCount(short count) {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(count);
        page.writeData(SLOT_COUNT_OFFSET, buffer.array());
        page.setDirty(true);
    }

    /**
     * 更新空闲空间指针
     * @param pointer 新的空闲空间指针
     */
    private void updateFreeSpacePointer(short pointer) {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(pointer);
        page.writeData(FREE_SPACE_OFFSET, buffer.array());
        page.setDirty(true);
    }

    /**
     * 获取指定槽的偏移量和长度
     * @param slotNum 槽号
     * @return 包含偏移量和长度的数组[offset, length]，如果槽为空则返回[-1, 0]
     */
    private short[] getSlotInfo(int slotNum) {
        short slotCount = getSlotCount();
        if (slotNum >= slotCount) {
            throw new IllegalArgumentException("槽号超出范围：" + slotNum);
        }

        int slotOffset = SLOT_DIRECTORY_OFFSET + slotNum * SLOT_SIZE;
        byte[] slotData = page.readData(slotOffset, SLOT_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(slotData);

        short offset = buffer.getShort();
        short length = buffer.getShort();

        return new short[] {offset, length};
    }

    /**
     * 更新指定槽的信息
     * @param slotNum 槽号
     * @param offset 记录偏移量
     * @param length 记录长度
     */
    private void updateSlotInfo(int slotNum, short offset, short length) {
        int slotOffset = SLOT_DIRECTORY_OFFSET + slotNum * SLOT_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_SIZE);
        buffer.putShort(offset);
        buffer.putShort(length);

        page.writeData(slotOffset, buffer.array());
        page.setDirty(true);
    }

    @Override
    public int insertRecord(byte[] data) throws Exception {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("记录数据不能为空");
        }

        // 检查数据长度，确保不超过短整型范围
        if (data.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("记录数据过大，超过" + Short.MAX_VALUE + "字节");
        }

        short dataLength = (short) data.length;

        // 计算所需空间
        int requiredSpace = dataLength;

        // 检查是否有足够空间
        if (getFreeSpace() < requiredSpace) {
            // 尝试整理页面后再检查
            compact();
            if (getFreeSpace() < requiredSpace) {
                throw new Exception("页面空间不足，无法插入记录");
            }
        }

        // 获取当前槽数量和空闲空间指针
        short slotCount = getSlotCount();
        short freePtr = getFreeSpacePointer();

        // 寻找可用槽或创建新槽
        int slotNum = -1;
        for (int i = 0; i < slotCount; i++) {
            short[] slotInfo = getSlotInfo(i);
            if (slotInfo[0] == SLOT_EMPTY) {
                slotNum = i;
                break;
            }
        }

        // 如果没有可用槽，创建新槽
        if (slotNum == -1) {
            slotNum = slotCount;
            updateSlotCount((short) (slotCount + 1));
        }

        // 计算新记录存储位置
        short newRecordOffset = (short) (freePtr - dataLength);

        // 更新槽信息
        updateSlotInfo(slotNum, newRecordOffset, dataLength);

        // 写入记录数据
        page.writeData(newRecordOffset, data);

        // 更新空闲空间指针
        updateFreeSpacePointer(newRecordOffset);

        return slotNum;
    }

    @Override
    public boolean deleteRecord(int slotNum) throws Exception {
        short slotCount = getSlotCount();
        if (slotNum < 0 || slotNum >= slotCount) {
            return false;
        }

        // 获取槽信息
        short[] slotInfo = getSlotInfo(slotNum);
        if (slotInfo[0] == SLOT_EMPTY) {
            return false;  // 槽已经为空
        }

        // 将槽标记为空
        updateSlotInfo(slotNum, SLOT_EMPTY, (short) 0);

        return true;
    }

    @Override
    public boolean updateRecord(int slotNum, byte[] newData) throws Exception {
        if (newData == null || newData.length == 0) {
            throw new IllegalArgumentException("记录数据不能为空");
        }

        short slotCount = getSlotCount();
        if (slotNum < 0 || slotNum >= slotCount) {
            return false;
        }

        // 获取原槽信息
        short[] slotInfo = getSlotInfo(slotNum);
        if (slotInfo[0] == SLOT_EMPTY) {
            return false;  // 槽为空，无法更新
        }

        short oldOffset = slotInfo[0];
        short oldLength = slotInfo[1];
        short newLength = (short) newData.length;

        // 如果新数据与旧数据长度相同，直接覆盖
        if (newLength == oldLength) {
            page.writeData(oldOffset, newData);
            return true;
        }

        // 否则，删除旧记录，插入新记录
        deleteRecord(slotNum);

        // 检查是否有足够空间
        if (getFreeSpace() < newLength) {
            // 尝试整理页面后再检查
            compact();
            if (getFreeSpace() < newLength) {
                throw new Exception("页面空间不足，无法更新记录");
            }
        }

        // 计算新记录存储位置
        short freePtr = getFreeSpacePointer();
        short newRecordOffset = (short) (freePtr - newLength);

        // 写入记录并更新槽信息
        page.writeData(newRecordOffset, newData);
        updateSlotInfo(slotNum, newRecordOffset, newLength);

        // 更新空闲空间指针
        updateFreeSpacePointer(newRecordOffset);

        return true;
    }

    @Override
    public byte[] getRecord(int slotNum) throws Exception {
        short slotCount = getSlotCount();
        if (slotNum < 0 || slotNum >= slotCount) {
            throw new IllegalArgumentException("槽号超出范围：" + slotNum);
        }

        // 获取槽信息
        short[] slotInfo = getSlotInfo(slotNum);
        if (slotInfo[0] == SLOT_EMPTY) {
            return null;  // 槽为空
        }

        // 读取记录数据
        short offset = slotInfo[0];
        short length = slotInfo[1];

        return page.readData(offset, length);
    }

    @Override
    public int getFreeSpace() {
        short slotCount = getSlotCount();
        short freePtr = getFreeSpacePointer();

        // 计算槽目录占用空间
        int directorySpace = SLOT_DIRECTORY_OFFSET + slotCount * SLOT_SIZE;

        // 可用空间 = 空闲指针 - 槽目录末尾
        return freePtr - directorySpace;
    }

    @Override
    public void compact() {
        short slotCount = getSlotCount();
        if (slotCount == 0) {
            return;  // 页面为空，无需整理
        }

        // 获取所有有效记录
        List<Integer> validSlots = new ArrayList<>();
        List<byte[]> records = new ArrayList<>();

        for (int i = 0; i < slotCount; i++) {
            short[] slotInfo = getSlotInfo(i);
            if (slotInfo[0] != SLOT_EMPTY) {
                validSlots.add(i);
                records.add(page.readData(slotInfo[0], slotInfo[1]));
            }
        }

        // 如果没有有效记录，重置页面
        if (validSlots.isEmpty()) {
            initializePage();
            return;
        }

        // 重新排列记录
        short newFreePtr = (short) page.getData().length;

        for (int i = 0; i < validSlots.size(); i++) {
            int slotNum = validSlots.get(i);
            byte[] record = records.get(i);

            // 计算新位置
            newFreePtr -= record.length;

            // 更新槽信息
            updateSlotInfo(slotNum, newFreePtr, (short) record.length);

            // 写入记录
            page.writeData(newFreePtr, record);
        }

        // 更新空闲空间指针
        updateFreeSpacePointer(newFreePtr);
    }

    @Override
    public Iterator<Integer> getValidSlots() {
        List<Integer> validSlots = new ArrayList<>();
        short slotCount = getSlotCount();

        for (int i = 0; i < slotCount; i++) {
            short[] slotInfo = getSlotInfo(i);
            if (slotInfo[0] != SLOT_EMPTY) {
                validSlots.add(i);
            }
        }

        return validSlots.iterator();
    }
}
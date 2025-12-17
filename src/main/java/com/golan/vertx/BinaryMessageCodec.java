package com.golan.vertx;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 高性能、零异常风险的二进制消息编解码器
 * 核心设计：位图(Bitmap)标记可选字段，严格边界检查
 */
public class BinaryMessageCodec {

    // 字段存在性位图常量
    private static final byte HAS_TO_BIT       = (byte) 0b10000000;
    private static final byte HAS_FROM_BIT     = (byte) 0b01000000;
    private static final byte HAS_PAYLOAD_BIT  = (byte) 0b00100000;
    private static final byte HAS_ID_BIT       = (byte) 0b00010000;
    private static final byte HAS_TO_TYPE_BIT   = (byte) 0b00001000;
    private static final byte HAS_FROM_TYPE_BIT = (byte) 0b00000100;
    private static final byte HAS_QOS_BIT      = (byte) 0b00000010;

    // 安全限制：防止恶意数据导致内存溢出 (例如限制单个字段最大 10MB)
    private static final int MAX_FIELD_SIZE = 10 * 1024 * 1024;

    /**
     * 编码：将 Message 对象转换为字节数组
     * 即使 msg 为空或字段异常，也会安全处理
     */
    public static byte[] encode(Message msg) {
        if (msg == null) return new byte[0];

        try {
            byte flags = 0;
            int varSize = 0;

            // 1. 预处理并计算变长字段长度
            byte[] idB = getBytes(msg.getId());
            byte[] toB = getBytes(msg.getTo());
            byte[] fromB = getBytes(msg.getFrom());
            byte[] payB = msg.getPayload();

            if (idB != null) { flags |= HAS_ID_BIT; varSize += 4 + idB.length; }
            if (toB != null) { flags |= HAS_TO_BIT; varSize += 4 + toB.length; }
            if (fromB != null) { flags |= HAS_FROM_BIT; varSize += 4 + fromB.length; }
            if (payB != null) { flags |= HAS_PAYLOAD_BIT; varSize += 4 + payB.length; }

            // 2. 处理可选的定长字段
            if (msg.getToType() != null) { flags |= HAS_TO_TYPE_BIT; varSize += 4; }
            if (msg.getFromType() != null) { flags |= HAS_FROM_TYPE_BIT; varSize += 4; }
            if (msg.getQos() != null) { flags |= HAS_QOS_BIT; varSize += 4; }

            // 3. 分配内存并写入
            ByteBuffer buffer = ByteBuffer.allocate(1 + varSize);
            buffer.put(flags);

            // 严格按照顺序写入
            if ((flags & HAS_TO_TYPE_BIT) != 0) buffer.putInt(msg.getToType());
            if ((flags & HAS_FROM_TYPE_BIT) != 0) buffer.putInt(msg.getFromType());
            if ((flags & HAS_QOS_BIT) != 0) buffer.putInt(msg.getQos());

            writeVarField(buffer, idB, (flags & HAS_ID_BIT) != 0);
            writeVarField(buffer, toB, (flags & HAS_TO_BIT) != 0);
            writeVarField(buffer, fromB, (flags & HAS_FROM_BIT) != 0);
            writeVarField(buffer, payB, (flags & HAS_PAYLOAD_BIT) != 0);

            return buffer.array();
        } catch (Exception e) {
            // 理论上不会走到这里，除非 JVM 内存耗尽
            return new byte[0];
        }
    }

    /**
     * 解码：将字节数组还原为 Message 对象
     * 包含严格的边界检查，防止任何 BufferUnderflowException 或脏数据攻击
     */
    public static Message decode(byte[] data) {
        if (data == null || data.length < 1) return new Message();

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte flags = buffer.get();
            Message msg = new Message();

            // 1. 读取定长字段 (确保有足够的 4 字节空间)
            if ((flags & HAS_TO_TYPE_BIT) != 0 && buffer.remaining() >= 4) msg.setToType(buffer.getInt());
            if ((flags & HAS_FROM_TYPE_BIT) != 0 && buffer.remaining() >= 4) msg.setFromType(buffer.getInt());
            if ((flags & HAS_QOS_BIT) != 0 && buffer.remaining() >= 4) msg.setQos(buffer.getInt());

            // 2. 读取变长字段 (String)
            msg.setId(readString(buffer, (flags & HAS_ID_BIT) != 0));
            msg.setTo(readString(buffer, (flags & HAS_TO_BIT) != 0));
            msg.setFrom(readString(buffer, (flags & HAS_FROM_BIT) != 0));

            // 3. 读取 Payload (byte[])
            if ((flags & HAS_PAYLOAD_BIT) != 0) {
                byte[] p = readRawBytes(buffer);
                if (p != null) msg.setPayload(p);
            }

            return msg;
        } catch (Exception e) {
            // 捕获所有潜在的解包异常，返回空对象而非抛出错误
            return new Message();
        }
    }

    // --- 私有辅助方法：确保安全性 ---

    private static byte[] getBytes(String s) {
        return (s == null || s.isEmpty()) ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    private static void writeVarField(ByteBuffer buffer, byte[] data, boolean exists) {
        if (exists && data != null) {
            buffer.putInt(data.length);
            buffer.put(data);
        }
    }

    private static String readString(ByteBuffer buffer, boolean exists) {
        byte[] b = readRawBytes(buffer, exists);
        return (b == null) ? null : new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] readRawBytes(ByteBuffer buffer) {
        return readRawBytes(buffer, true);
    }

    private static byte[] readRawBytes(ByteBuffer buffer, boolean exists) {
        if (!exists || buffer.remaining() < 4) return null;

        int len = buffer.getInt();

        // 防御性检查：长度必须为正，且不能超过缓冲区剩余大小，且不能超过安全上限
        if (len <= 0 || len > buffer.remaining() || len > MAX_FIELD_SIZE) {
            return null;
        }

        byte[] b = new byte[len];
        buffer.get(b);
        return b;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Message {
        private String id;
        private String to;
        private Integer toType;
        private String from;
        private Integer fromType;
        private Integer qos;
        private byte[] payload;
    }
}
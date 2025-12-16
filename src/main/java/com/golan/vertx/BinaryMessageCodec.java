package com.golan.vertx;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinaryMessageCodec {

    // 标志位常量 (保持 byte 类型，用于位操作)
    private static final byte HAS_TO_BIT = (byte) 0b10000000;      // Bit 7: 标记 'to' 字段是否存在
    private static final byte HAS_FROM_BIT = (byte) 0b01000000;    // Bit 6: 标记 'from' 字段是否存在
    private static final byte HAS_PAYLOAD_BIT = (byte) 0b00100000; // Bit 5: 标记 'payload' 字段是否存在

    // 序列化 (Encode)
    public static byte[] encode(Message msg) {

        byte flags = 0;

        // 1. 确定字符串和 Payload 的 byte[] 形式
        byte[] toBytes = null;
        if (msg.getTo() != null) {
            toBytes = msg.getTo().getBytes(StandardCharsets.UTF_8);
        }

        byte[] fromBytes = null;
        if (msg.getFrom() != null) {
            fromBytes = msg.getFrom().getBytes(StandardCharsets.UTF_8);
        }

        byte[] payload = msg.getPayload();

        // 2. 计算 flags 和变长字段的总长度
        int variableLength = 0;

        // 只有非空且长度大于 0 的字段才会被写入，并设置标志位
        if (toBytes != null && toBytes.length > 0) {
            flags |= HAS_TO_BIT;
            variableLength += 4 + toBytes.length; // 长度(4) + 内容
        }

        if (fromBytes != null && fromBytes.length > 0) {
            flags |= HAS_FROM_BIT;
            variableLength += 4 + fromBytes.length; // 长度(4) + 内容
        }

        if (payload != null && payload.length > 0) {
            flags |= HAS_PAYLOAD_BIT;
            variableLength += 4 + payload.length; // 长度(4) + 内容
        }

        // 3. 计算总长度 (定长部分 + 变长部分)
        // [Flags(1)] + [toType(4)] + [fromType(4)] + [qos(4)] + [Variable Length Part]
        // ⚠️ 定长部分现在是 1 + 4 + 4 + 4 = 13 字节
        int fixedLength = 1 + 4 + 4 + 4;
        int totalLength = fixedLength + variableLength;

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // --- 写入固定头部 (13 字节) ---
        buffer.put(flags);              // 1️⃣ Flags (byte)

        buffer.putInt(msg.getToType());    // 2️⃣ toType (int)
        buffer.putInt(msg.getFromType());  // 3️⃣ fromType (int)
        buffer.putInt(msg.getQos());       // 4️⃣ qos (int)

        // --- 写入变长字段 (根据 Flags 标记判断) ---

        // 5️⃣ to (String)
        if ((flags & HAS_TO_BIT) != 0) {
            buffer.putInt(toBytes.length);
            buffer.put(toBytes);
        }

        // 6️⃣ from (String)
        if ((flags & HAS_FROM_BIT) != 0) {
            buffer.putInt(fromBytes.length);
            buffer.put(fromBytes);
        }

        // 7️⃣ payload (byte[])
        if ((flags & HAS_PAYLOAD_BIT) != 0) {
            buffer.putInt(payload.length);
            buffer.put(payload);
        }

        return buffer.array();
    }

    // 反序列化 (Decode)
    public static Message decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // 1️⃣ 读取 Flags 字节
        byte flags = buffer.get();

        // 2️⃣ 读取定长字段
        int toType = buffer.getInt();
        int fromType = buffer.getInt();
        int qos = buffer.getInt();

        String to = null;
        String from = null;
        byte[] payload = null;

        // 3️⃣ 读取变长字段 (根据 Flags 判断)

        // 4️⃣ to (String)
        if ((flags & HAS_TO_BIT) != 0) {
            int toLen = buffer.getInt();
            byte[] toBytes = new byte[toLen];
            buffer.get(toBytes);
            to = new String(toBytes, StandardCharsets.UTF_8);
        }

        // 5️⃣ from (String)
        if ((flags & HAS_FROM_BIT) != 0) {
            int fromLen = buffer.getInt();
            byte[] fromBytes = new byte[fromLen];
            buffer.get(fromBytes);
            from = new String(fromBytes, StandardCharsets.UTF_8);
        }

        // 6️⃣ payload (byte[])
        if ((flags & HAS_PAYLOAD_BIT) != 0) {
            int payloadLen = buffer.getInt();
            payload = new byte[payloadLen];
            buffer.get(payload);
        }

        // 7️⃣ 返回 Message
        return new Message(to, toType, from, fromType, qos, payload);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    // 消息类
    public static class Message {
        private String to;
        private int toType;     // 调整为 int (4 bytes)
        private String from;
        private int fromType;   // 调整为 int (4 bytes)
        private int qos;        // 调整为 int (4 bytes)
        private byte[] payload;
    }
}
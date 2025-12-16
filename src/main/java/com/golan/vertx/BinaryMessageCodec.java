package com.golan.vertx;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinaryMessageCodec {

    // 序列化 (Encode)
    public static byte[] encode(Message msg) {

        // 字符串字段编码为 byte[]
        byte[] toBytes = msg.getTo().getBytes(StandardCharsets.UTF_8);
        byte[] fromBytes = msg.getFrom().getBytes(StandardCharsets.UTF_8);
        byte[] payload = msg.getPayload();

        // -------------------------------------------------------------------
        // ⚠️ 计算总长度：注意 int (4) 和 String 长度 (4) 的变化
        // [toType(4)] + [fromType(4)] + [qos(4)]
        // + [toLen(4) + toBytes.length]
        // + [fromLen(4) + fromBytes.length]
        // + [payloadLen(4) + payload.length]
        // -------------------------------------------------------------------
        int totalLength =
                4 + // toType (int)
                4 + // fromType (int)
                4 + // qos (int)
                4 + toBytes.length + // to (String length + bytes)
                4 + fromBytes.length + // from (String length + bytes)
                4 + payload.length;    // payload (byte[] length + bytes)

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // 1️⃣ toType (int)
        buffer.putInt(msg.getToType());

        // 2️⃣ fromType (int)
        buffer.putInt(msg.getFromType());

        // 3️⃣ qos (int)
        buffer.putInt(msg.getQos());

        // 4️⃣ to (String)
        buffer.putInt(toBytes.length);
        buffer.put(toBytes);

        // 5️⃣ from (String)
        buffer.putInt(fromBytes.length);
        buffer.put(fromBytes);

        // 6️⃣ payload (byte[])
        buffer.putInt(payload.length);
        buffer.put(payload);

        return buffer.array();
    }

    // 反序列化 (Decode)
    public static Message decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // 1️⃣ toType (int)
        int toType = buffer.getInt();

        // 2️⃣ fromType (int)
        int fromType = buffer.getInt();

        // 3️⃣ qos (int)
        int qos = buffer.getInt();

        // 4️⃣ to (String)
        int toLen = buffer.getInt();
        byte[] toBytes = new byte[toLen];
        buffer.get(toBytes);
        String to = new String(toBytes, StandardCharsets.UTF_8);

        // 5️⃣ from (String)
        int fromLen = buffer.getInt();
        byte[] fromBytes = new byte[fromLen];
        buffer.get(fromBytes);
        String from = new String(fromBytes, StandardCharsets.UTF_8);

        // 6️⃣ payload (byte[])
        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);

        return new Message(to, toType, from, fromType, qos, payload);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    // 消息类
    public static class Message {
        private String to;
        private int toType;   // 调整为 int (4 bytes)
        private String from;
        private int fromType; // 调整为 int (4 bytes)
        private int qos;      // 调整为 int (4 bytes)
        private byte[] payload;
    }
}
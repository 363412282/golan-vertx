package com.golan.vertx;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinaryMessageCodec {

    // 序列化
    public static byte[] encode(Message msg) {
        byte[] receiverBytes = msg.getReceiver().getBytes(StandardCharsets.UTF_8);
        byte[] payload = msg.getPayload();

        int totalLength = 4 + 4 + receiverBytes.length + 4 + 4 + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // 1️⃣ receiverType
        buffer.putInt(msg.getReceiverType());

        // 2️⃣ receiver
        buffer.putInt(receiverBytes.length);
        buffer.put(receiverBytes);

        // 3️⃣ qos
        buffer.putInt(msg.getQos());

        // 4️⃣ payload
        buffer.putInt(payload.length);
        buffer.put(payload);

        return buffer.array();
    }

    // 反序列化
    public static Message decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        int receiverType = buffer.getInt();

        int receiverLen = buffer.getInt();
        byte[] receiverBytes = new byte[receiverLen];
        buffer.get(receiverBytes);
        String receiver = new String(receiverBytes, StandardCharsets.UTF_8);

        int qos = buffer.getInt();

        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);

        return new Message(receiver, receiverType, qos, payload);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    // 消息类
    public static class Message {
        private String receiver;
        private int receiverType;
        private int qos;
        private byte[] payload;

    }
}

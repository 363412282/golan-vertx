package com.golan.vertx;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 高性能二进制消息编解码器 - 最终优化版
 * 特点：零异常风险、支持可选字段、支持空 Payload 传输、严格边界检查
 */
public class BinaryMessageCodec {

    // 字段存在性位图常量 (使用单字节 flags，目前剩余 2 位可扩展)
    private static final byte HAS_TO_BIT       = (byte) 0b10000000;
    private static final byte HAS_FROM_BIT     = (byte) 0b01000000;
    private static final byte HAS_PAYLOAD_BIT  = (byte) 0b00100000;
    private static final byte HAS_ID_BIT       = (byte) 0b00010000;
    private static final byte HAS_QOS_BIT      = (byte) 0b00001000;
    private static final byte HAS_TOPIC_BIT    = (byte) 0b00000100;

    // 安全限制：防止坏数据导致 OOM (单个字段限制 10MB)
    private static final int MAX_FIELD_SIZE = 10 * 1024 * 1024;

    /**
     * 编码：将 Message 转换为字节数组
     * 允许 msg 对象及其内部所有字段为 null
     */
    public static byte[] encode(Message msg) {
        if (msg == null) return new byte[0];

        try {
            byte flags = 0;
            int totalSize = 1; // 初始 1 字节用于 flags

            // 1. 预处理变长字段并计算总长度
            byte[] idB    = getBytes(msg.getId());
            byte[] toB    = getBytes(msg.getTo());
            byte[] fromB  = getBytes(msg.getFrom());
            byte[] topicB = getBytes(msg.getTopic());
            byte[] payB   = msg.getPayload();

            if (idB != null)    { flags |= HAS_ID_BIT;      totalSize += 4 + idB.length; }
            if (toB != null)    { flags |= HAS_TO_BIT;      totalSize += 4 + toB.length; }
            if (fromB != null)  { flags |= HAS_FROM_BIT;    totalSize += 4 + fromB.length; }
            if (topicB != null) { flags |= HAS_TOPIC_BIT;   totalSize += 4 + topicB.length; }
            if (payB != null)   { flags |= HAS_PAYLOAD_BIT; totalSize += 4 + payB.length; }

            // 2. 处理定长字段 (QoS)
            if (msg.getQos() != null) { flags |= HAS_QOS_BIT; totalSize += 4; }

            // 3. 写入数据
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            buffer.put(flags);

            // 优先写入定长字段
            if ((flags & HAS_QOS_BIT) != 0) buffer.putInt(msg.getQos());

            // 按序写入变长字段
            writeVarField(buffer, idB,    (flags & HAS_ID_BIT) != 0);
            writeVarField(buffer, toB,    (flags & HAS_TO_BIT) != 0);
            writeVarField(buffer, fromB,  (flags & HAS_FROM_BIT) != 0);
            writeVarField(buffer, topicB, (flags & HAS_TOPIC_BIT) != 0);
            writeVarField(buffer, payB,   (flags & HAS_PAYLOAD_BIT) != 0);

            return buffer.array();
        } catch (Exception e) {
            // 兜底策略：发生任何非预期异常时返回空数组，不中断主流程
            return new byte[0];
        }
    }

    /**
     * 解码：将字节数组还原为 Message 对象
     * 具备严格边界检查，能够处理截断数据
     */
    public static Message decode(byte[] data) {
        if (data == null || data.length < 1) return new Message();

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte flags = buffer.get();
            Message msg = new Message();

            // 1. 读取定长字段 (QoS)
            if ((flags & HAS_QOS_BIT) != 0) {
                if (buffer.remaining() >= 4) {
                    msg.setQos(buffer.getInt());
                } else {
                    return msg; // 数据不全，提前返回已解析部分
                }
            }

            // 2. 依次读取变长字符串字段
            msg.setId(readString(buffer,    (flags & HAS_ID_BIT) != 0));
            msg.setTo(readString(buffer,    (flags & HAS_TO_BIT) != 0));
            msg.setFrom(readString(buffer,  (flags & HAS_FROM_BIT) != 0));
            msg.setTopic(readString(buffer, (flags & HAS_TOPIC_BIT) != 0));

            // 3. 读取二进制 Payload
            if ((flags & HAS_PAYLOAD_BIT) != 0) {
                byte[] p = readRawBytes(buffer, true);
                if (p != null) msg.setPayload(p);
            }

            return msg;
        } catch (Exception e) {
            // 解析失败时返回空对象，防止上游业务 NPE
            return new Message();
        }
    }

    // --- 私有工具方法 ---

    private static byte[] getBytes(String s) {
        // null 或空字符串在传输时不占用空间
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

    private static byte[] readRawBytes(ByteBuffer buffer, boolean exists) {
        if (!exists) return null;

        // 边界检查：必须至少有 4 字节来读取长度
        if (buffer.remaining() < 4) return null;

        int len = buffer.getInt();

        // 安全检查：长度不能为负，不能超过剩余缓冲区，不能超过 10MB 阈值
        if (len < 0 || len > buffer.remaining() || len > MAX_FIELD_SIZE) {
            return null;
        }

        // 处理空数组场景 (len == 0)
        if (len == 0) return new byte[0];

        byte[] b = new byte[len];
        buffer.get(b);
        return b;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Message {
        private String id;      // 消息唯一标识
        private String to;      // 接收者
        private String from;    // 发送者
        private Integer qos;    // 服务质量
        private String topic;   // 订阅主题
        private byte[] payload; // 二进制载荷
    }
}
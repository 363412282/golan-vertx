package com.golan.vertx;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.redis.client.*;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.digests.Blake2bDigest;

import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class Main extends AbstractVerticle {

    private RedisAPI redisAPI;
    private Redis redis;

    private final Map<String, List<ServerWebSocket>> userConnections = new ConcurrentHashMap<>();

    private final Map<String, MqttEndpoint> clientConnections = new ConcurrentHashMap<>();
    private final Map<String, String> pendingAckMap = new ConcurrentHashMap<>();




    private static final String WS_CHANNEL = "/server/ws:7";
    private static final String WS_QUEUE = "/user/ws:7";
    private static final String MQTT_CHANNEL = "/server/mqtt:7";
    private static final String MQTT_QUEUE = "/device/mqtt:7";

    private SnowflakeId snowflakeId;

    public static void main(String[] args) {

        int instances = Runtime.getRuntime().availableProcessors();

        Vertx vertx = Vertx.vertx();
        DeploymentOptions options = new DeploymentOptions().setInstances(instances);

        vertx.deployVerticle(Main.class.getName(), options)
            .onFailure(cause -> {
                System.err.println("Vertx Deploy Verticle Failed: " + cause.getMessage());
            });
    }

    @Override
    public void start(Promise<Void> startPromise) {

        snowflakeId = snowflakeId(7L, 31L);

        Future.all(
            startMqttServer(),
            startWebSocketServer(),
            startRedisListener()
        ).onSuccess(v -> {
            System.out.println("ALL Services Started (MQTT: 1883, WS: 8383, Redis Listener: " + MQTT_CHANNEL + ", " + WS_CHANNEL + ")");
            startPromise.complete();
        }).onFailure(cause -> {
            System.err.println("Services Start failed.: " + cause.getMessage());
            startPromise.fail(cause);
        });
    }

    private Future<Void> startRedisListener() {
        // 假设 Redis 运行在 localhost:6379
        RedisOptions config = new RedisOptions().setConnectionString("redis://127.0.0.1:6379");
        redis = Redis.createClient(vertx, config);
        redisAPI = RedisAPI.api(redis);
        return redis.connect().compose(conn -> {
            // 设置消息处理程序
            conn.handler(response -> {
                if (response != null && response.size() == 3 && "message".equalsIgnoreCase(response.get(0).toString())) {
                    String channel = response.get(1).toString();
                    byte[] messageBytes = response.get(2).toBuffer().getBytes();
                    if (WS_CHANNEL.equals(channel)) {
                        handleSubWSMessage(messageBytes);
                    } else if (MQTT_CHANNEL.equals(channel)) {
                        handleSubMQTTMessage(messageBytes);
                    }
                }
            });

            return conn.send(Request.cmd(Command.SUBSCRIBE)
                    .arg(WS_CHANNEL)
                    .arg(MQTT_CHANNEL))
                    .compose(res -> {
                        System.out.println("Redis Channel Subscribed. (" + WS_CHANNEL + ", " + MQTT_CHANNEL + ")");
                        return Future.succeededFuture();
                    });
        });
    }

    private Future<HttpServer> startWebSocketServer() {
       return vertx.createHttpServer()
        .webSocketHandler(ws -> {
            System.out.println("[WS] Client Connected: " + ws.remoteAddress());

            Map<String, String> params = parseQuery(ws.query());
            String sessionToken = params.get("token");

            if (sessionToken == null || sessionToken.isEmpty() ) {
                System.out.println("[WS] Session Token Empty. Close Connection.");
                ws.close();
                return;
            }

            System.out.println("[WS] Session Token: " + sessionToken);

            Long sid = parseSessionId(sessionToken, "Laurel");
            if (sid == null ) {
                System.out.println("[WS] Parse Session ID Failed. Close Connection.");
                ws.close();
                return;
            }

            redisAPI.hgetall("s:" + Long.toString(sid, 36))
                .onSuccess(response -> {
                    String tempUserId = null;
                    if (response != null && response.get("u") != null) {
                        tempUserId = response.get("u").toString();
                    }
                    if (tempUserId != null && !tempUserId.isEmpty()) {

                        final String userId = tempUserId;

                        userConnections.computeIfAbsent(userId, k -> new CopyOnWriteArrayList<>()).add(ws);

                        System.out.println("[WS] Session Authenticated. User ID: " + userId);

                        ws.handler(buffer -> {
                            String msg = buffer.toString(StandardCharsets.UTF_8);

                            byte[] messageBytes = BinaryMessageCodec.encode(BinaryMessageCodec.Message.builder()
                                    .payload(buffer.getBytes())
                                    .fromType(1)
                                    .from(userId)
                                    .build());

                            Request request = Request.cmd(Command.LPUSH)
                                    .arg(Buffer.buffer(WS_QUEUE))
                                    .arg(Buffer.buffer(messageBytes));

                            redis.send(request)
                                    .onSuccess(res -> System.out.println("[MQTT] Upstream Message To Queue: " + MQTT_QUEUE))
                                    .onFailure(e -> System.err.println("[MQTT] Upstream Message Failed. " + e.getMessage()));

                            System.out.println("[WS] Message From User (" + userId + "): " + msg);
                        });

                        ws.closeHandler(v -> {
                            List<ServerWebSocket> connections = userConnections.get(userId);
                            if (connections != null) {
                                connections.remove(ws);
                                if (connections.isEmpty()) {
                                    userConnections.remove(userId);
                                }
                            }
                            System.out.println("[WS] Close Handler. Connection Removed. User ID: " + userId);
                        });
                    } else {
                        System.out.println("[WS] Session Auth Failed. Session Token: " + sessionToken + ", Session ID: " + sid);
                        ws.close();
                    }
                })
                .onFailure(cause -> {
                    System.err.println("[WS] Session Redis HGET Failed. Session Token: " + sessionToken + ". Close Connection. " + cause.getMessage());
                    ws.close();
                });

        })
        .listen(8383)
        .onSuccess(server -> {
            System.out.println("[WS] WebSocket Server Started. Listen 8383");
        })
        .onFailure(cause -> {
            System.err.println("[WS] WebSocket Server Start Failed. " + cause.getMessage());
        });
    }

    private Future<MqttServer> startMqttServer() {
        MqttServer mqttServer = MqttServer.create(vertx);

        return mqttServer.endpointHandler(endpoint -> {
            String clientId = endpoint.clientIdentifier();
            System.out.println("[MQTT] Client Connected: " + clientId);

            // 1. 注册会话
            clientConnections.put(clientId, endpoint);

            // 2. 连接断开处理
            endpoint.disconnectHandler(v -> handleDisconnect(clientId));
            endpoint.closeHandler(v -> handleDisconnect(clientId));

            // 3. 处理客户端的订阅请求 (修复 MqttSubAckReasonCode 问题)
            endpoint.subscribeHandler(subscribe -> {
                // 在 Vert.x 5.x 中，直接使用 List<MqttQoS> 即可
                List<MqttQoS> grantedQoS = subscribe.topicSubscriptions().stream()
                        .map(MqttTopicSubscription::qualityOfService)
                        .collect(Collectors.toList());

                // ack the subscriptions request
                endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQoS);

                System.out.println("[MQTT] Client (" + clientId + ") Subscribe " + subscribe.topicSubscriptions().size() + " Topic.");
            });

            // 3.1 处理客户端的消息
            endpoint.publishHandler(message -> {

                String topic = message.topicName();
                MqttQoS qos = message.qosLevel();
                int messageId = message.messageId();
                byte[] payload = message.payload().getBytes();

                System.out.println("[MQTT] Message From Client (" + clientId + "). Topic: " + topic + ", QoS: " + qos);

                // 消息确认逻辑：回复 PUBACK (QoS 1) 或 PUBREC (QoS 2)
                if (qos == MqttQoS.AT_LEAST_ONCE) {
                    endpoint.publishAcknowledge(messageId);
                } else if (qos == MqttQoS.EXACTLY_ONCE) {
                    endpoint.publishReceived(messageId);
                }

                byte[] messageBytes = BinaryMessageCodec.encode(BinaryMessageCodec.Message.builder()
                        .qos(qos.value())
                        .payload(payload)
                        .from(clientId)
                        .fromType(0)
                        .build());

                Request request = Request.cmd(Command.LPUSH)
                        .arg(Buffer.buffer(MQTT_QUEUE))
                        .arg(Buffer.buffer(messageBytes));

                redis.send(request)
                    .onSuccess(res -> System.out.println("[MQTT] Upstream Message To Queue: " + MQTT_QUEUE))
                    .onFailure(e -> System.err.println("[MQTT] Upstream Message Failed. " + e.getMessage()));

            }).publishReleaseHandler(messageId -> {
                // QoS 2 流程的第二步: 服务器回复 PUBCOMP 完成握手
                endpoint.publishComplete(messageId);
                System.out.println("[MQTT] Receive QoS 2 PUBREL, Response PUBCOMP");
            });

            // 4. 处理 QoS 1 ACK (PUBACK)
            endpoint.publishAcknowledgeHandler(packetId -> {
                confirmAndRemoveFromRedis(clientId, packetId, "QoS 1 (PUBACK)");
            });

            // 5. 处理 QoS 2 ACK 流程
            // 5.1 收到 PUBREC -> 服务器回复 PUBREL
            endpoint.publishReceivedHandler(packetId -> {
                endpoint.publishRelease(packetId);
                System.out.println("[MQTT] Receive QoS 2 PUBREC (Packet: " + packetId + "), Response PUBREL");
            });

            // 5.2 收到 PUBCOMP -> 服务器从 Redis 删除备份
            endpoint.publishCompletionHandler(packetId -> {
                confirmAndRemoveFromRedis(clientId, packetId, "QoS 2 (PUBCOMP)");
            });

            // 6. 接受连接
            endpoint.accept(false);

            // 7. 检查是否有离线消息并补发
            checkAndSendOfflineMessages(endpoint);

        }).listen(1883)
        .onSuccess(s -> System.out.println("[MQTT] Server Started. Listen 1883"))
        .onFailure(e -> System.err.println("[MQTT] Server Start Failed. " + e.getMessage()));
    }


    private void handleSubWSMessage(byte[] messageBytes) {
        try {
            BinaryMessageCodec.Message message = BinaryMessageCodec.decode(messageBytes);

            if (message.getTo() != null && message.getPayload() != null) {

                // 关键修改：获取连接列表
                List<ServerWebSocket> connections = userConnections.get(message.getTo());

                if (connections != null && !connections.isEmpty()) {
                    int sentCount = 0;

                    // 遍历列表，向所有连接发送消息
                    for (ServerWebSocket ws : connections) {
                        if (!ws.isClosed()) {
                            ws.writeTextMessage(new String(message.getPayload(), StandardCharsets.UTF_8));
                            // ws.writeBinaryMessage(Buffer.buffer(message.getPayload()));
                            sentCount++;
                        } else {
                            // 可选：如果发现连接已关闭但仍在列表中，可以考虑将其移除
                            connections.remove(ws);
                        }
                    }

                    System.out.println("[WS] Handle Redis PUB/SUB Message. User ID (" + message.getTo() + ") Sent: " + sentCount);

                    if (connections.isEmpty()) {
                        userConnections.remove(message.getTo());
                    }

                } else {
                    System.out.println("[WS] Handle Redis PUB/SUB Message Failed. Empty Connections. User ID (" + message.getTo() + ")");
                }
            }
        } catch (Exception e) {
            System.err.println("[WS] Handle Redis PUB/SUB Message Error. " + e.getMessage() + ", Message Length: " + (messageBytes != null ? messageBytes.length : 0));
        }
    }

    private Long parseSessionId(String token36, String key) {

        try {
            BigInteger number = new BigInteger(token36, 36);

            String token16 = number.toString(16);

            String sidHex = token16.substring(9, token16.length() - 8);
            String sig = token16.substring(0, 9) + token16.substring(token16.length() - 8);

            Long id = Long.parseLong(sidHex, 16);

            String sig2 = "1" + blake2bDigest(id + key, 64);

            if (!Objects.equals(sig, sig2)) {
                return null;
            }

            return id;

        } catch (NumberFormatException e) {
            System.err.println("[WS] Parse Session Id, NumberFormatException: " + token36);
            return null;
        }
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) return params;

        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                String key = URLDecoder.decode(kv[0], StandardCharsets.UTF_8);
                String value = URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                params.put(key, value);
            }
        }
        return params;
    }

    private String blake2bDigest(String str, int digestSize) {
        Blake2bDigest blake2bDigest = new Blake2bDigest(digestSize);
        byte[] in = str.getBytes();
        blake2bDigest.update(in, 0, in.length);
        final byte[] out = new byte[blake2bDigest.getDigestSize()];
        blake2bDigest.doFinal(out, 0);
        return Hex.encodeHexString(out);
    }






    
    private void handleSubMQTTMessage(byte[] messageBytes) {
        try {
            BinaryMessageCodec.Message message = BinaryMessageCodec.decode(messageBytes);

            String targetClientId = message.getTo();
            byte[] payload = message.getPayload();
            int qosLevel = message.getQos();

            String redisMsgId = Long.toString(snowflakeId.nextId(), 36);

            MqttEndpoint endpoint = clientConnections.get(targetClientId);
            boolean isOnline = (endpoint != null && endpoint.isConnected());

            // --- 策略 1: QoS 0 (发后即忘) ---
            if (qosLevel == 0) {
                if (isOnline) {
                    sendMqttMessage(endpoint, null, payload, MqttQoS.AT_MOST_ONCE);
                } else {
                    System.out.println("[MQTT] QoS 0 Message Discarded: Client Offline (" + targetClientId + ")");
                }
                return;
            }

            // --- 策略 2: QoS 1 & 2 (先持久化，再发送) ---

            Request request = Request.cmd(Command.HSET)
                    .arg(Buffer.buffer("mqtt:msg:" + targetClientId))
                    .arg(Buffer.buffer(redisMsgId))
                    .arg(Buffer.buffer(messageBytes));

            redis.send(request)
                .onSuccess(res -> {
                    if (isOnline) {
                        sendMqttMessage(endpoint, redisMsgId, payload, MqttQoS.valueOf(qosLevel));
                    } else {
                        System.out.println("[MQTT] QoS " + qosLevel + " Message Persisted: Client Offline (" + targetClientId + ")");
                    }
                })
                .onFailure(e -> System.err.println("[MQTT] QoS " + qosLevel + " Message Persisted Failed: Client ID (" + targetClientId + ")"));

        } catch (Exception e) {
            System.err.println("[MQTT] Handle Redis PUB/SUB Message Error. " + e.getMessage() + ", Message Length: " + (messageBytes != null ? messageBytes.length : 0));
        }
    }

    private void sendMqttMessage(MqttEndpoint endpoint, String redisMsgId, byte[] payload, MqttQoS qos) {
        // 调用不带 handler 的 publish 方法，它返回 Future<Integer>

        String topic = "/device/" + endpoint.clientIdentifier();

        endpoint.publish(
            topic,
            Buffer.buffer(payload),
            qos,
            false,
            false
        )
        // 成功时执行 (onSuccess 接收 Integer 类型的 mqttPacketId)
        .onSuccess(mqttPacketId -> {
            if (qos != MqttQoS.AT_MOST_ONCE && redisMsgId != null) {
                String mapKey = getMapKey(endpoint.clientIdentifier(), mqttPacketId);
                pendingAckMap.put(mapKey, redisMsgId);
                System.out.println("[MQTT] QoS " + qos.value() + " Message Sent. PacketId=" + mqttPacketId + ", Topic=" + topic);
            }
        })
        // 失败时执行 (onFailure 接收 Throwable 类型的 cause)
        .onFailure(cause -> {
            // 如果发送失败，消息仍在 Redis 中，等待客户端重连
            System.err.println("[MQTT] QoS " + qos.value() + " Message Send Failed. Topic=" + topic + ". " + cause.getMessage());
        });
    }

    private void checkAndSendOfflineMessages(MqttEndpoint endpoint) {
        String clientId = endpoint.clientIdentifier();

        Request request = Request.cmd(Command.HGETALL).arg(Buffer.buffer("mqtt:msg:" + clientId));

        redis.send(request)
            .onSuccess(response -> {
                if (response == null || response.size() == 0) return;

                System.out.println("[MQTT] Client (" + clientId + ") Offline Message Count: " + (response.size() / 2) + ". Start Send.");

                for (int i = 0; i < response.size(); i += 2) {
                    String redisMsgId = response.get(i).toString();

                    try {
                        byte[] messageBytes = response.get(i + 1).toBuffer().getBytes();

                        BinaryMessageCodec.Message message = BinaryMessageCodec.decode(messageBytes);

                        String targetClientId = message.getTo();
                        byte[] payload = message.getPayload();
                        int qosLevel = message.getQos();

                        sendMqttMessage(endpoint, redisMsgId, payload, MqttQoS.valueOf(qosLevel));

                    } catch (Exception e) {
                        // 解析失败，立即删除坏数据
                        redisAPI.hdel(Arrays.asList("mqtt:msg:" + clientId, redisMsgId));

                        System.err.println("[MQTT] Client (" + clientId + ") Offline Message Parse Failed And Delete. " + e.getMessage());
                    }
                }
            })
            .onFailure(e -> System.err.println("[MQTT] Client (" + clientId + ") Offline Message Fetch Error. " + e.getMessage()));
    }

    private void confirmAndRemoveFromRedis(String clientId, int packetId, String context) {
        String mapKey = getMapKey(clientId, packetId);
        String redisMsgId = pendingAckMap.remove(mapKey);

        if (redisMsgId != null) {
            // 使用 redisAPI.hdel (Vert.x 5.x 接收 List<String>)
            redisAPI.hdel(Arrays.asList("mqtt:msg:" + clientId, redisMsgId))
                    .onSuccess(res -> System.out.println("[MQTT] ACK [" + context + "] And Redis Message Deleted. Client=" + clientId + ", MsgId=" + packetId))
                    .onFailure(e -> System.err.println("[MQTT] ACK [" + context + "] And Delete Redis Message Failed. " + e.getMessage()));
        } else {
            System.out.println("[MQTT] ACK [" + context + "] And Redis Message ID Not In Pending Ack Map. Client=" + clientId + ", PacketId=" + packetId);
        }
    }

    // 辅助方法
    private void handleDisconnect(String clientId) {
        clientConnections.remove(clientId);
        System.out.println("[MQTT] Remove Client (" + clientId + ") From Connections.");
    }

    private String getMapKey(String clientId, int packetId) {
        return clientId + ":" + packetId;
    }


    private SnowflakeId snowflakeId(long dataCenterId, long machineId) {

        long epoch = 1737784400000L;

        int timestampBits = 44;
        int machineBits = 7;
        int dataCenterBits = 3;
        int sequenceBits = 9;

        return new SnowflakeId(epoch, timestampBits, machineBits, dataCenterBits, sequenceBits, dataCenterId, machineId);
    }
}




























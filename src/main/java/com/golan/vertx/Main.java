package com.golan.vertx;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.client.WebClient;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.redis.client.*;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.digests.Blake2bDigest;

import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;


@Slf4j
public class Main extends AbstractVerticle {

    private RedisAPI redisAPI;
    private Redis redis;

    private WebClient webClient;

    private final Map<String, List<ServerWebSocket>> userConnections = new ConcurrentHashMap<>();

    private final Map<String, MqttEndpoint> clientConnections = new ConcurrentHashMap<>();
    private final Map<String, String> pendingAckMap = new ConcurrentHashMap<>();


    private static final String WS_CHANNEL = "/server/ws";
    private static final String MQTT_CHANNEL = "/server/mqtt";




    public static void main(String[] args) {

        int instances = Runtime.getRuntime().availableProcessors();

        Vertx vertx = Vertx.vertx();

        DeploymentOptions mainOptions = new DeploymentOptions().setInstances(instances);

        DeploymentOptions redisOptions = new DeploymentOptions().setInstances(1);

        Future.all(
            vertx.deployVerticle(Main.class.getName(), mainOptions),
            vertx.deployVerticle(Main.RedisListenerVerticle.class.getName(), redisOptions)
        ).onFailure(cause -> {
            log.error("Vertx Deploy Failed: {}", cause.getMessage());
        });
    }




    @Slf4j
    public static class RedisListenerVerticle extends AbstractVerticle {

        @Override
        public void start(Promise<Void> startPromise) {

            RedisOptions config = new RedisOptions().setConnectionString("redis://127.0.0.1:6379");
            Redis redis = Redis.createClient(vertx, config);

            redis.connect().compose(conn -> {

                conn.handler(response -> {
                    if (response != null && response.size() == 3 && "message".equalsIgnoreCase(response.get(0).toString())) {
                        String channel = response.get(1).toString();
                        byte[] messageBytes = response.get(2).toBuffer().getBytes();

                        if (WS_CHANNEL.equals(channel)) {

                            String to = BinaryMessageCodec.decodeTo(messageBytes);

                            if (to != null) {
                                vertx.eventBus().send("/ws/user/" + to, messageBytes);
                            }

                        } else if (MQTT_CHANNEL.equals(channel)) {

                            String to = BinaryMessageCodec.decodeTo(messageBytes);

                            if (to != null) {
                                vertx.eventBus().send("/mqtt/client/" + to, messageBytes);
                            }
                        }
                    }
                });

                return conn.send(Request.cmd(Command.SUBSCRIBE)
                    .arg(WS_CHANNEL)
                    .arg(MQTT_CHANNEL))
                    .compose(res -> {
                        log.info("[START] Redis Channel Subscribed, ({}, {})", WS_CHANNEL, MQTT_CHANNEL);
                        return Future.succeededFuture();
                    });
            });
        }
    }













    @Override
    public void start(Promise<Void> startPromise) {

        RedisOptions config = new RedisOptions().setConnectionString("redis://127.0.0.1:6379")
                .setMaxPoolSize(4)
                .setPoolCleanerInterval(5000);

        this.redis = Redis.createClient(vertx, config);
        this.redisAPI = RedisAPI.api(this.redis);

        this.webClient = WebClient.create(vertx);

        Future.all(
            startMqttServer(),
            startWebSocketServer()
        ).onSuccess(v -> {
            startPromise.complete();
            log.info("[START] ALL Services Started (MQTT: 1983, WS: 8383, Redis Listener: {}, {})", WS_CHANNEL, MQTT_CHANNEL);
        }).onFailure(cause -> {
            log.error("[START] Services Start Failed, {}", cause.getMessage());
        });
    }

    private Future<HttpServer> startWebSocketServer() {
       return vertx.createHttpServer()
        .webSocketHandler(ws -> {
            log.info("[WS] Client Connected: {}", ws.remoteAddress());

            Map<String, String> params = parseQuery(ws.query());
            String sessionToken = params.get("token");

            if (sessionToken == null || sessionToken.isEmpty() ) {
                log.info("[WS] Session Token Empty, Close Connection");
                ws.close();
                return;
            }

            log.info("[WS] Session Token: {}", sessionToken);

            Long sid = parseSessionId(sessionToken, "Laurel");
            if (sid == null ) {
                log.info("[WS] Parse Session ID Failed, Close Connection");
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

                        // 1. 注册用户连接

                        final String userId = tempUserId;

                        log.info("[WS] Session Authenticated. User ID: {}", userId);

                        userConnections.computeIfAbsent(userId, k -> new CopyOnWriteArrayList<>()).add(ws);

                        MessageConsumer<byte[]> consumer = vertx.eventBus().consumer("/ws/user/" + userId, msg -> {
                            handleWSMessage(ws, msg.body());
                        });

                        // 2. 连接断开处理
                        ws.closeHandler(v -> {
                            consumer.unregister();
                            List<ServerWebSocket> connections = userConnections.get(userId);
                            if (connections != null) {
                                connections.remove(ws);
                                if (connections.isEmpty()) {
                                    userConnections.remove(userId);
                                }
                            }
                            log.info("[WS] Close Handler. Connection Removed. User ID: {}", userId);
                        });

                        // 3. 处理客户端的消息
                        ws.handler(buffer -> {

                            BinaryMessageCodec.Message message = BinaryMessageCodec.Message.builder()
                                    .from(userId)
                                    .payload(buffer.getBytes())
                                    .build();

                            byte[] messageBytes = BinaryMessageCodec.encode(message);

                            webClient.post(8585, "127.0.0.1", "/system/upstream/ws")
                                    .putHeader("Content-Type", "application/octet-stream")
                                    .sendBuffer(Buffer.buffer(messageBytes))
                                    .onSuccess(res -> {
                                        log.info("[WS] Message From User ({}) Upstream OK. Length={}", userId, message.getPayload().length);
                                    })
                                    .onFailure(err -> {
                                        log.error("[WS] Message From User ({}) Upstream Failed. Length={}", userId, message.getPayload().length);
                                    });
                        });


                    } else {
                        log.info("[WS] Session Auth Failed. Session Token: {}, Session ID: {}", sessionToken, sid);
                        ws.close();
                    }
                })
                .onFailure(cause -> {
                    log.error("[WS] Session Redis Failed. Session Token: {}. Close Connection. {}", sessionToken, cause.getMessage());
                    ws.close();
                });

        })
        .listen(8383)
        .onSuccess(server -> {
            log.info("[WS] WebSocket Server Started. Listen 8383");
        })
        .onFailure(cause -> {
            log.error("[WS] WebSocket Server Start Failed. {}", cause.getMessage());
        });
    }

    private Future<MqttServer> startMqttServer() {

        MqttServer mqttServer = MqttServer.create(vertx);

        return mqttServer.endpointHandler(endpoint -> {
            String clientId = endpoint.clientIdentifier();
            log.info("[MQTT] Client Connected: {}", clientId);

            // 1. 注册会话
            MqttEndpoint old =clientConnections.put(clientId, endpoint);
            if (old != null) {
                old.close();
            }

            MessageConsumer<byte[]> consumer = vertx.eventBus().consumer("/mqtt/client/" + clientId, msg -> {
                handleMQTTMessage(endpoint, msg.body());
            });

            // 2. 连接断开处理
            endpoint.disconnectHandler(v -> {
                consumer.unregister();
                clientConnections.remove(clientId);
                log.info("[MQTT] Remove Client ({}) From Connections", clientId);
            });
            endpoint.closeHandler(v -> {
                consumer.unregister();
                clientConnections.remove(clientId);
                log.info("[MQTT] Remove Client ({}) From Connections", clientId);
            });

            // 3. 处理客户端的订阅请求 (修复 MqttSubAckReasonCode 问题)
            endpoint.subscribeHandler(subscribe -> {
                // 在 Vert.x 5.x 中，直接使用 List<MqttQoS> 即可
                List<MqttQoS> grantedQoS = subscribe.topicSubscriptions().stream()
                        .map(MqttTopicSubscription::qualityOfService)
                        .collect(Collectors.toList());

                // ack the subscriptions request
                endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQoS);

                log.info("[MQTT] Client ({}) Subscribe {} Topic", clientId, subscribe.topicSubscriptions().size());
            });

            // 3.1 处理客户端的消息
            endpoint.publishHandler(msg -> {

                String topic = msg.topicName();
                MqttQoS qos = msg.qosLevel();
                int messageId = msg.messageId();
                byte[] payload = msg.payload().getBytes();

                // 处理 QoS 流程
                if (qos == MqttQoS.AT_LEAST_ONCE) {
                    endpoint.publishAcknowledge(messageId);
                } else if (qos == MqttQoS.EXACTLY_ONCE) {
                    endpoint.publishReceived(messageId);
                }

                BinaryMessageCodec.Message message = BinaryMessageCodec.Message.builder()
                        .from(clientId)
                        .topic(topic)
                        .payload(payload)
                        .qos(qos.value())
                        .build();

                byte[] messageBytes = BinaryMessageCodec.encode(message);

                webClient.post(8585, "127.0.0.1", "/system/upstream/mqtt")
                        .putHeader("Content-Type", "application/octet-stream")
                        .sendBuffer(Buffer.buffer(messageBytes))
                        .onSuccess(res -> {
                            log.info("[MQTT] Message From Client ({}) Upstream OK. Length={}", clientId, message.getPayload().length);
                        })
                        .onFailure(err -> {
                            log.error("[MQTT] Message From Client ({}) Upstream Failed. Length={}", clientId, message.getPayload().length);
                        });

            }).publishReleaseHandler(messageId -> {
                // QoS 2 流程的第二步: 服务器回复 PUBCOMP 完成握手
                endpoint.publishComplete(messageId);
                log.info("[MQTT] Receive QoS 2 PUBREL, Response PUBCOMP");
            });

            // 4. 处理 QoS 1 ACK (PUBACK) -> 服务器从 Redis 删除备份
            endpoint.publishAcknowledgeHandler(packetId -> {
                confirmAndRemoveFromRedis(clientId, packetId, "QoS 1 (PUBACK)");
            });

            // 5. 处理 QoS 2 ACK 流程

            // 5.1 收到 PUBREC -> 服务器回复 PUBREL
            endpoint.publishReceivedHandler(packetId -> {
                endpoint.publishRelease(packetId);
                log.info("[MQTT] Receive QoS 2 PUBREC (Packet: {}), Response PUBREL", packetId);
            });

            // 5.2 收到 PUBCOMP -> 服务器从 Redis 删除备份
            endpoint.publishCompletionHandler(packetId -> {
                confirmAndRemoveFromRedis(clientId, packetId, "QoS 2 (PUBCOMP)");
            });

            // 6. 接受连接
            endpoint.accept(false);

            // 7. 检查是否有离线消息并补发
            checkAndSendOfflineMessages(endpoint);

        })
        .listen(1983)
        .onSuccess(s -> log.info("[MQTT] Server Started. Listen 1983"))
        .onFailure(e -> log.error("[MQTT] Server Start Failed, {}", e.getMessage()));
    }


    private void handleWSMessage(ServerWebSocket ws, byte[] messageBytes) {
        try {
            BinaryMessageCodec.Message message = BinaryMessageCodec.decode(messageBytes);

            if (message.getTo() == null || message.getTo().isEmpty() || message.getPayload() == null) {
                return;
            }

            if (!ws.isClosed()) {
                ws.writeTextMessage(new String(message.getPayload(), StandardCharsets.UTF_8));
                // ws.writeBinaryMessage(Buffer.buffer(message.getPayload()));

                log.info("[WS] Message Sent. User={}, Length={}", message.getTo(), message.getPayload().length);

            } else {
                String userId = message.getTo();
                List<ServerWebSocket> connections = userConnections.get(userId);
                connections.remove(ws);
                if (connections.isEmpty()) {
                    userConnections.remove(userId);
                }
            }

        } catch (Exception e) {
            log.error("[WS] Handle WS Message Error: {}, Length={}", e.getMessage(), (messageBytes != null ? messageBytes.length : 0));
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
            log.error("[WS] Parse Session Id, NumberFormatException: {}", token36);
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
        return HexFormat.of().formatHex(out);
    }






    
    private void handleMQTTMessage(MqttEndpoint endpoint, byte[] messageBytes) {

        try {

            BinaryMessageCodec.Message message = BinaryMessageCodec.decode(messageBytes);

            if (message.getId() == null || message.getId().isEmpty()
                    || message.getTopic() == null || message.getTopic().isEmpty()
                    || message.getTo() == null || message.getTo().isEmpty()
                    || message.getPayload() == null) {
                return;
            }

            if (message.getQos() == null) {
                message.setQos(MqttQoS.AT_MOST_ONCE.value());
            }

            byte[] payload = message.getPayload();
            Integer qosLevel = message.getQos();
            String redisMsgId = message.getId();
            String topic = message.getTopic();

            if (message.getQos().equals(MqttQoS.EXACTLY_ONCE.value()) || message.getQos().equals(MqttQoS.AT_LEAST_ONCE.value()) )  {

                String redisKey = "mqtt:msg:" + message.getTo();

                List<Request> batch = new ArrayList<>();
                batch.add(Request.cmd(Command.HSET)
                        .arg(Buffer.buffer(redisKey))
                        .arg(Buffer.buffer(message.getId()))
                        .arg(Buffer.buffer(messageBytes)));
                batch.add(Request.cmd(Command.EXPIRE)
                        .arg(Buffer.buffer(redisKey))
                        .arg(Buffer.buffer(String.valueOf(3600 * 36))));
                batch.add(Request.cmd(Command.HLEN)
                        .arg(Buffer.buffer(redisKey)));

                redis.batch(batch)
                    .onSuccess(res -> {

                        sendMqttMessage(endpoint, redisMsgId, topic, payload, MqttQoS.valueOf(qosLevel));

                        Response hLenRes = res.get(2);
                        long messageCount = (hLenRes != null) ? hLenRes.toLong() : 0L;
                        if (messageCount >= 7) {
                            log.error("[MQTT] Client ({}) Has {} Offline Messages", message.getTo(), messageCount);
                        }
                    })
                    .onFailure(e -> log.error("[MQTT] QoS {} Message Persisted Failed: Client ID ({})", message.getQos(), message.getTo()));

            } else if (message.getQos().equals(MqttQoS.AT_MOST_ONCE.value())) {

                sendMqttMessage(endpoint, redisMsgId, topic, payload, MqttQoS.valueOf(qosLevel));
            }
        } catch (Exception e) {
            log.error("[MQTT] Handle MQTT Message Error: {}, Length={}", e.getMessage(), (messageBytes != null ? messageBytes.length : 0));
        }
    }

    private void sendMqttMessage(MqttEndpoint endpoint, String redisMsgId, String topic, byte[] payload, MqttQoS qos) {

        if (endpoint == null || !endpoint.isConnected()) {
            return;
        }

        if (qos == MqttQoS.EXACTLY_ONCE || qos == MqttQoS.AT_LEAST_ONCE) {

            redisAPI.set(Arrays.asList("mqtt:lock:" + redisMsgId, "1", "NX", "EX", "60"))
                .onSuccess(res -> {
                    if (res != null && "OK".equals(res.toString())) {
                        endpoint.publish(
                            topic,
                            Buffer.buffer(payload),
                            qos,
                            false,
                            false
                        )
                        // 成功时执行 (onSuccess 接收 Integer 类型的 mqttPacketId)
                        .onSuccess(packetId -> {
                            if (redisMsgId != null) {
                                String mapKey = endpoint.clientIdentifier() + ":" + packetId;
                                pendingAckMap.put(mapKey, redisMsgId);
                            }
                            log.info("[MQTT] QoS {} Message Sent, PacketId={}, Topic={}, Length={}", qos.value(), packetId, topic, payload.length);
                        })
                        // 失败时执行 (onFailure 接收 Throwable 类型的 cause)
                        .onFailure(cause -> {
                            // 如果发送失败，消息仍在 Redis 中，等待客户端重连
                            log.error("[MQTT] QoS {} Message Send Failed, Topic={}, Error={}", qos.value(), topic, cause.getMessage());
                        });
                    } else {
                        log.error("[MQTT] QoS {} Message Lock Failed. Topic={}, MessageId={}", qos.value(), topic, redisMsgId);
                    }
                });

        } else if (qos == MqttQoS.AT_MOST_ONCE ) {
            endpoint.publish(
                topic,
                Buffer.buffer(payload),
                qos,
                false,
                false
            )
            .onSuccess(mqttPacketId -> {
                log.info("[MQTT] QoS {} Message Sent, PacketId=" + mqttPacketId + ", Topic={}, Length={}", qos.value(), topic, payload.length);
            })
            .onFailure(cause -> {
                log.error("[MQTT] QoS {} Message Send Failed, Topic={}, Error={}", qos.value(), topic, cause.getMessage());
            });
        }
    }

    private void checkAndSendOfflineMessages(MqttEndpoint endpoint) {
        String clientId = endpoint.clientIdentifier();

        String redisKey = "mqtt:msg:" + clientId;

        Request request = Request.cmd(Command.HGETALL).arg(Buffer.buffer(redisKey));

        redis.send(request)
            .onSuccess(response -> {
                if (response == null || response.size() == 0) return;

                log.info("[MQTT] Client ({}) Offline Message Count={}, Start Send", clientId, response.getKeys().size());

                for (String redisMsgId : response.getKeys()) {

                    Response value = response.get(redisMsgId);
                    if (value == null) {
                        redisAPI.hdel(Arrays.asList(redisKey, redisMsgId));
                        continue;
                    }

                    byte[] messageBytes = value.toBuffer().getBytes();
                    BinaryMessageCodec.Message message = BinaryMessageCodec.decode(messageBytes);

                    byte[] payload = message.getPayload();
                    Integer qosLevel = message.getQos();
                    String topic = message.getTopic();

                    if (qosLevel == null || payload == null) {
                        redisAPI.hdel(Arrays.asList(redisKey, redisMsgId));
                        continue;
                    }

                    sendMqttMessage(endpoint, redisMsgId, topic, payload, MqttQoS.valueOf(qosLevel));
                }
            })
            .onFailure(e -> log.error("[MQTT] Client ({}) Offline Message Fetch Error={}", clientId, e.getMessage()));
    }

    private void confirmAndRemoveFromRedis(String clientId, int packetId, String context) {
        String mapKey = clientId + ":" + packetId;
        String redisMsgId = pendingAckMap.remove(mapKey);

        if (redisMsgId != null) {

            redisAPI.hdel(Arrays.asList("mqtt:msg:" + clientId, redisMsgId))
                    .onSuccess(res -> log.info("[MQTT] ACK [{}] And Redis Message Deleted. Client={}, PacketId={}",context, clientId, packetId))
                    .onFailure(e -> log.error("[MQTT] ACK [{}] And Delete Redis Message Failed, Error={}", context, e.getMessage()));
        } else {
            log.info("[MQTT] ACK [{}] And Redis Message ID Not In Pending Ack Map, Client={}, PacketId={}", context, clientId, packetId);
        }
    }

}

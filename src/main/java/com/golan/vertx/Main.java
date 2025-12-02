package com.golan.vertx;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.messages.codes.MqttSubAckReasonCode;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.digests.Blake2bDigest;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Main extends AbstractVerticle {

    private Redis redis;

    private final Map<String, List<ServerWebSocket>> userConnections = new ConcurrentHashMap<>();

    private static final String REDIS_CHANNEL = "/server/7";

    public static void main(String[] args) {

        int instances = Runtime.getRuntime().availableProcessors();

        Vertx vertx = Vertx.vertx();
        DeploymentOptions options = new DeploymentOptions().setInstances(instances);

        vertx.deployVerticle(Main.class.getName(), options)
            .onFailure(cause -> {
                System.err.println("Vertx.deployVerticle failed: " + cause.getMessage());
            });
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Future.all(
            // startMqttServer(),
            startWebSocketServer(),
            startRedisListener()
        ).onSuccess(v -> {
            System.out.println("所有服务已成功启动 (MQTT: 1883, WS: 8080, Redis Listener: " + REDIS_CHANNEL + ")");
            startPromise.complete();
        }).onFailure(cause -> {
            System.err.println("服务启动失败: " + cause.getMessage());
            startPromise.fail(cause);
        });
    }


    private Future<Void> startRedisListener() {
        // 假设 Redis 运行在 localhost:6379
        RedisOptions config = new RedisOptions().setConnectionString("redis://127.0.0.1:6379");
        redis = Redis.createClient(vertx, config);

        return redis.connect().compose(conn -> {
            // 设置消息处理程序
            conn.handler(response -> {
                if (response != null && response.size() == 3 && "message".equalsIgnoreCase(response.get(0).toString())) {
                    String channel = response.get(1).toString();
                    String payload = response.get(2).toString();

                    if (REDIS_CHANNEL.equals(channel)) {
                        handleTargetedMessage(payload);
                    }
                }
            });

            // 订阅 REDIS_CHANNEL 频道
            return conn.send(Request.cmd(Command.SUBSCRIBE).arg(REDIS_CHANNEL))
                    .compose(res -> {
                        System.out.println("成功订阅 Redis 频道: " + REDIS_CHANNEL);
                        return Future.succeededFuture();
                    });
        });
    }


    private Future<HttpServer> startWebSocketServer() {
       return vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    System.out.println("-> WebSocket client connected: " + ws.remoteAddress());

                    String cookieHeader = ws.headers().get("Cookie");
                    String sessionToken = extractSessionIdFromCookie(cookieHeader);

                    // ... (Session ID 和路径校验，保持不变) ...

                    System.out.println("-> WebSocket connected, Session ID: " + sessionToken);

                    // 1. 校验 Session ID
                    if (sessionToken == null || sessionToken.isEmpty() ) {
                        System.out.println("   [WS] 未找到 session token cookie，拒绝连接。");
                        ws.close();
                        return;
                    }

                    Long sid = parseId(sessionToken, "Laurel");
                    if (sid == null ) {
                        System.out.println("   [WS] 未找到 session id，拒绝连接。");
                        ws.close();
                        return;
                    }

                    redis.send(Request.cmd(Command.HGETALL).arg("s:" + Long.toString(sid, 36)))
                            .onSuccess(response -> {

                                String tempUserId = null;
                                if (response != null && response.get("u") != null) {
                                    tempUserId = response.get("u").toString();
                                }

                                if (tempUserId != null && !tempUserId.isEmpty()) {

                                    final String userId = tempUserId;

                                    // 3. **认证成功**：在 Event Loop 线程中继续连接管理

                                    // 原子地添加连接到列表
                                    userConnections.computeIfAbsent(userId, k -> new CopyOnWriteArrayList<>()).add(ws);
                                    System.out.println("   [WS] Session authenticated. User ID: " + userId);

                                    ws.handler(buffer -> {
                                        String msg = buffer.toString(StandardCharsets.UTF_8);
                                        System.out.println("<- Received from WS: " + msg);
                                        // ws.writeTextMessage("Server received your WS message: " + msg);
                                    });

                                    // 4. 关闭时移除连接
                                    ws.closeHandler(v -> {
                                        List<ServerWebSocket> connections = userConnections.get(userId);
                                        if (connections != null) {
                                            connections.remove(ws);
                                            if (connections.isEmpty()) {
                                                userConnections.remove(userId);
                                            }
                                        }
                                        System.out.println("<- WebSocket closed. User ID removed: " + userId);
                                    });



                                } else {
                                    // 找不到有效的 User ID
                                    System.out.println("   [WS] Session ID " + sessionToken + " 对应的 User ID 无效，关闭连接。");
                                    ws.close();
                                }
                            })
                            .onFailure(cause -> {
                                // 6. Redis 查询失败 (例如连接错误)
                                System.err.println("Redis HGET 查询失败: " + cause.getMessage() + "，关闭连接。");
                                ws.close();
                            });




                })
                // Vert.x 5.x listen 方法返回 Future，这里我们直接使用 onSuccess/onFailure
                .listen(8383)
                .onSuccess(server -> {
                    System.out.println("WebSocket Server 正在监听端口 8383。");
                })
                .onFailure(cause -> {
                    System.err.println("启动 WebSocket Server 失败: " + cause.getMessage());
                });
    }

    private Future<MqttServer> startMqttServer() {
        MqttServer mqttServer = MqttServer.create(vertx);

        return mqttServer.endpointHandler(endpoint -> {


            endpoint.pingHandler(v -> {


                System.out.println("Ping received from client");

            });


            endpoint.subscribeHandler(subscribe -> {


                List<MqttSubAckReasonCode> reasonCodes = new ArrayList<>();

                for (MqttTopicSubscription s : subscribe.topicSubscriptions()) {

                    System.out.println("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());

                    reasonCodes.add(MqttSubAckReasonCode.qosGranted(s.qualityOfService()));

                }

                // ack the subscriptions request

                endpoint.subscribeAcknowledge(subscribe.messageId(), reasonCodes, MqttProperties.NO_PROPERTIES);


                endpoint.publish("/server/s:7",

                        Buffer.buffer("Hello from the Vert.x MQTT server"),

                        MqttQoS.EXACTLY_ONCE,

                        false,

                        false);

            });


            endpoint.unsubscribeHandler(unsubscribe -> {


                for (String t : unsubscribe.topics()) {

                    System.out.println("Unsubscription for " + t);

                }

                // ack the subscriptions request

                endpoint.unsubscribeAcknowledge(unsubscribe.messageId());

            });


            endpoint.publishHandler(message -> {


                System.out.println("Just received message [" + message.payload().toString(Charset.defaultCharset()) + "] with QoS [" + message.qosLevel() + "]");


                if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {

                    endpoint.publishAcknowledge(message.messageId());

                } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {

                    endpoint.publishReceived(message.messageId());

                }


            }).publishReleaseHandler(messageId -> {


                endpoint.publishComplete(messageId);

            });


            // specifing handlers for handling QoS 1 and 2


            endpoint.publishAcknowledgeHandler(messageId -> {


                System.out.println("Received ack for message = " + messageId);


            }).publishReceivedHandler(messageId -> {


                endpoint.publishRelease(messageId);


            }).publishCompletionHandler(messageId -> {


                System.out.println("Received ack for message = " + messageId);

            });

            // shows main connect info

            System.out.println("MQTT client [" + endpoint.clientIdentifier() + "] request to connect, clean session = " + endpoint.isCleanSession());


            if (endpoint.auth() != null) {

                System.out.println("[username = " + endpoint.auth().getUsername() + ", password = " + endpoint.auth().getPassword() + "]");

            }

            System.out.println("[properties = " + endpoint.connectProperties() + "]");

            if (endpoint.will() != null) {

                System.out.println("[will topic = " + endpoint.will().getWillTopic() +

                        " QoS = " + endpoint.will().getWillQos() + " isRetain = " + endpoint.will().isWillRetain() + "]");

            }


            System.out.println("[keep alive timeout = " + endpoint.keepAliveTimeSeconds() + "]");


            // accept connection from the remote client

            endpoint.accept(false);

        })

        .listen(1883)
        .onSuccess(server -> {
            System.out.println("MQTT Server 正在监听端口 1883。");
        })
        .onFailure(cause -> {
            System.err.println("启动 MQTT Server 失败: " + cause.getMessage());
        });
    }


    private String extractSessionIdFromCookie(String cookieHeader) {
        if (cookieHeader == null) {
            return null;
        }
        // 简单分隔，查找 sessionid=
        String[] cookies = cookieHeader.split("; ");
        for (String cookie : cookies) {
            if (cookie.startsWith("SID=")) {
                // 确保 sessionid 后面没有多余的 =
                return cookie.substring("SID=".length());
            }
        }
        return null;
    }

    private void handleTargetedMessage(String jsonPayload) {
        try {
            JsonObject message = new JsonObject(jsonPayload);
            String userId = message.getString("userId");
            String data = message.getString("data");

            if (userId != null && data != null) {

                // 关键修改：获取连接列表
                List<ServerWebSocket> connections = userConnections.get(userId);

                if (connections != null && !connections.isEmpty()) {
                    int sentCount = 0;

                    // 遍历列表，向所有连接发送消息
                    for (ServerWebSocket ws : connections) {
                        if (!ws.isClosed()) {
                            ws.writeTextMessage(data);
                            sentCount++;
                        } else {
                            // 可选：如果发现连接已关闭但仍在列表中，可以考虑将其移除
                            connections.remove(ws);
                        }
                    }

                    System.out.println("   [Redis Bridge] 成功发送消息到 Session ID: " + userId + "，连接数: " + sentCount);

                    if (connections.isEmpty()) {
                        userConnections.remove(userId);
                    }

                } else {
                    System.out.println("   [Redis Bridge] 目标 Session ID 不在线或连接已关闭: " + userId);
                }
            }
        } catch (Exception e) {
            System.err.println("解析 Redis 消息失败: " + e.getMessage() + ", Payload: " + jsonPayload);
        }
    }



    private Long parseId(String token36, String key) {

        BigInteger number = new BigInteger(token36, 36);

        String token16 = number.toString(16);

        String sidHex = token16.substring(9, token16.length() - 8);
        String sig = token16.substring(0, 9) + token16.substring(token16.length() - 8);

        Long id = null;

        try {
            id = Long.parseLong(sidHex, 16);
        } catch (NumberFormatException e) {
            System.err.println("UIDService.parseId, NumberFormatException: " + token36);
        }
        if (id == null) {
            return null;
        }

        String sig2 = "1" + blake2bDigest(String.valueOf(id) + key, 64);

        if (!Objects.equals(sig, sig2)) {
            return null;
        }

        return id;
    }

    private String blake2bDigest(String str, int digestSize) {
        Blake2bDigest blake2bDigest = new Blake2bDigest(digestSize);
        byte[] in = str.getBytes();
        blake2bDigest.update(in, 0, in.length);
        final byte[] out = new byte[blake2bDigest.getDigestSize()];
        blake2bDigest.doFinal(out, 0);
        return Hex.encodeHexString(out);
    }
}




























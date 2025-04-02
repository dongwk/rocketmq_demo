package internal;

import cn.hutool.core.thread.ThreadUtil;
import com.cloud.common.utils.DateUtils;
import com.cloud.oms.provincial.distribution.service.IProvincialDistributionWarehouseService;
import com.cloud.oms.provincial.internal.service.IInternalTradeOrderDropShippingBizService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.IOUtils;
import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.consumer.*;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.example.ProducerSingleton;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 测试
 *
 * @author dongwk
 * @date 2024-09-12 15:01
 */
@Slf4j
public class InternalOrderTest {

//    @Autowired
    private IProvincialDistributionWarehouseService warehouseService;

//    @Autowired
    private IInternalTradeOrderDropShippingBizService orderBizService;

//    @Autowired
//    private MQTransactionListener mqTransactionListener;      // 事务消息监听器
    // 消息生产者配置信息
    private String groupName = "rocketmq_demo";                 // 集群名称，这边以应用名称作为集群名称
    private String pNamesrvAddr = "10.10.23.139:9876";          // 生产者nameservice地址
    private Integer maxMessageSize = 4096;                      // 消息最大大小，默认4M
    private Integer sendMsgTimeout = 30000;                     // 消息发送超时时间，默认3秒
    private Integer retryTimesWhenSendFailed = 2;               // 消息发送失败重试次数，默认2次
    private ExecutorService executor = ThreadUtil.newExecutor(32); // 执行任务的线程池

    /**
     * 普通消息生产者.
     */
    @Test
    public void sendMq() throws Exception {
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8080;xxx:8081。
        String endpoint = "10.10.23.139:8080";
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "TopicTest"; // 普通消息
//        String topic = "TopicDelay"; // 延时消息
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();
        // 普通消息发送。
        Message message = provider.newMessageBuilder()
                .setTopic(topic)
                // 设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys("messageKey")
                // 设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag("messageTag")
                // 消息体。
                .setBody("messageBody".getBytes())
                .build();
        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
        } catch (ClientException e) {
            log.error("Failed to send message", e);
        }
        // producer.close();
    }

    /**
     * push模式消息消费者.
     * 普通消息，延迟消息，顺序消息用同一套都接收
     */
    @Test
    public void receiveMqPush() throws Exception {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = "10.10.23.139:8080";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoints)
                .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "FIFOGroup";

        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
//        String topic = "TopicTest"; // 普通消息
//        String topic = "TopicDelay"; // 延时消息
        String topic = "TopicFifo"; // 顺序消息
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // 设置消费者分组。
                .setConsumerGroup(consumerGroup)
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                // 设置消费监听器。
                .setMessageListener(messageView -> {
                    // 处理消息并返回消费结果。
                    log.info("Consume message successfully, messageId={}, messageBody={}", messageView.getMessageId(), StandardCharsets.UTF_8.decode(messageView.getBody()));
                    return ConsumeResult.SUCCESS;
                })
                .build();
        Thread.sleep(Long.MAX_VALUE);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        // pushConsumer.close();
    }

    /**
     * 延时消息.
     */
    @Test
    public void sendDelayMq() throws Exception {
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8080;xxx:8081。
        String endpoint = "10.10.23.139:8080";
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "TopicDelay";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .build();

        //定时/延时消息发送
        MessageBuilder messageBuilder = new MessageBuilderImpl();;
        //以下示例表示：延迟时间为10分钟之后的Unix时间戳。
        Long deliverTimeStamp = System.currentTimeMillis() + 5 * 1000;
        Message message = messageBuilder.setTopic(topic)
                //设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys("messageKey")
                //设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag("messageTag")
                .setDeliveryTimestamp(deliverTimeStamp)
                //消息体
                .setBody(("延迟消息 delay messageBody " + DateUtils.getTime()).getBytes())
                .build();
        try {
            //发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            System.out.println(sendReceipt.getMessageId());
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 顺序消息.
     */
    @Test
    public void sendFifoMq() throws Exception {
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8080;xxx:8081。
        String endpoint = "10.10.23.139:8080";
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "TopicFifo";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .build();

        //顺序消息发送。
        MessageBuilder messageBuilder = new MessageBuilderImpl();;
        Message message = messageBuilder.setTopic(topic)
                //设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys("messageKey")
                //设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag("messageTag")
                //设置顺序消息的排序分组，该分组尽量保持离散，避免热点排序分组。
                .setMessageGroup("fifoGroup001")
                //消息体。
                .setBody(("顺序消息 fifo messageBody " + DateUtils.getTime()).getBytes())
                .build();
        try {
            //发送消息，需要关注发送结果，并捕获失败等异常
            SendReceipt sendReceipt = producer.send(message);
            System.out.println(sendReceipt.getMessageId());
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * sample模式消息消费者.
     * 原子接口，可灵活自定义。
     * 可参考img.png对比
     *
     * 消费者三种分类
     * https://rocketmq.apache.org/zh/docs/featureBehavior/06consumertype
     */
    @Test
    public void receiveMqSample() throws Exception {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        // Credential provider is optional for client configuration.
        String accessKey = "yourAccessKey";
        String secretKey = "yourSecretKey";
        SessionCredentialsProvider sessionCredentialsProvider =
                new StaticSessionCredentialsProvider(accessKey, secretKey);

        String endpoints = pNamesrvAddr;
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoints)
//                .setCredentialProvider(sessionCredentialsProvider)
                .build();
        String consumerGroup = groupName;
        Duration awaitDuration = Duration.ofSeconds(30);
        String tag = "TagA";
        String topic = "TopicTest";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // In most case, you don't need to create too many consumers, singleton pattern is recommended.
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // Set the consumer group name.
                .setConsumerGroup(consumerGroup)
                // set await duration for long-polling.
                .setAwaitDuration(awaitDuration)
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .build();
        // Max message num for each long polling.
        int maxMessageNum = 16;
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(15);
        // Receive message, multi-threading is more recommended.
        do {
            final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
            log.info("Received {} message(s)", messages.size());
            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                try {
                    consumer.ack(message);
                    log.info("Message is acknowledged successfully, messageId={}", messageId);
                } catch (Throwable t) {
                    log.error("Message is failed to be acknowledged, messageId={}", messageId, t);
                }
            }
        } while (true);
        // Close the simple consumer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        // consumer.close();
    }

    /**
     * 延时消息，自定义时间.
     */
    @Test
    public void sendDeayMqWithSelfDefTime() throws Exception {

        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        String topic = "yourDelayTopic";
        final Producer producer = ProducerSingleton.getInstance(topic);
        // Define your message body.
        byte[] body = "This is a delay message for Apache RocketMQ".getBytes(StandardCharsets.UTF_8);
        String tag = "yourMessageTagA";
        Duration messageDelayTime = Duration.ofSeconds(10);
        final org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
                // Set topic for the current message.
                .setTopic(topic)
                // Message secondary classifier of message besides topic.
                .setTag(tag)
                // Key(s) of the message, another way to mark message besides message id.
                .setKeys("yourMessageKey-3ee439f945d7")
                // Set expected delivery timestamp of message.
                .setDeliveryTimestamp(System.currentTimeMillis() + messageDelayTime.toMillis())
                .setBody(body)
                .build();
        try {
            final SendReceipt sendReceipt = producer.send(message);
            log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
        } catch (Throwable t) {
            log.error("Failed to send message", t);
        }
    }
}

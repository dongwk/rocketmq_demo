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
        try {
            for (int i = 0; i < 1; i++) {
                // 普通消息发送。
                Message message = provider.newMessageBuilder()
                        .setTopic(topic)
                        // 设置消息索引键，可根据关键字精确查找某条消息。
                        .setKeys("messageKey")
                        // 设置消息Tag，用于消费端根据指定Tag过滤消息。
                        .setTag("messageTag")
                        // 消息体。
                        .setBody(("及时消息 messageBody "+(i+1)+" time:"+ DateUtils.getTime()).getBytes())
                        .build();
                // 发送消息，需要关注发送结果，并捕获失败等异常。
                SendReceipt sendReceipt = producer.send(message);
                log.info("Send message successfully, num={}, messageId={}", (i+1), sendReceipt.getMessageId());
            }
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
        String tag = "messageTag";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 为消费者指定所属的消费者分组，Group需要提前创建。
            String consumerGroup = "YourConsumerGroup"; // 普通消费组
//        String consumerGroup = "FIFOGroup"; // 顺序消费组
//        String consumerGroup = "rocketmq_demo"; // 普通消费组

        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "TopicTest"; // 普通消息
//        String topic = "TopicDelay"; // 延时消息
//        String topic = "TopicFifo"; // 顺序消息
//        String topic = "%DLQ%YourConsumerGroup"; // 死信消息
        // 关于死信消息的几点：
        // 1、死信消息，手动重发，手动重发后死信消息也不会删除，有效期后会自动删除，但是我这边验的都已经很久之前的了还不会删除，待定吧
        // 2、死信队列的处理，虽然叫死信队列，单是应该是死信消息 https://help.aliyun.com/zh/apsaramq-for-rocketmq/cloud-message-queue-rocketmq-4-x-series/user-guide/dead-letter-queues
        //
        // 3、我自己测的死信消息重发，这个是4月8号发的私信消息，今天是4月13号了都没删除，不知道是不是默认有效期的问题，有效期是有默认配置的3天，没时间去看这个默认配置了
        // 11:32:48.375 [RocketmqMessageConsumption-0-13] INFO internal.InternalOrderTest -- Consume message successfully, messageId=010A002700000B7B380806A69700000000, messageBody=及时消息 messageBody 1 time:2025-04-08 19:46:31
        //===================
        // 11:32:52.235 [RocketmqMessageConsumption-0-14] INFO internal.InternalOrderTest -- Consume message successfully, messageId=010A002700000B92940807EDC400000000, messageBody=及时消息 messageBody 1 time:2025-04-09 19:02:28
        // 4、死信消息重试，我不管订阅死信的topic还是原本的topic都能消费，不知道因为什么
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // 设置消费者分组。
                .setConsumerGroup(consumerGroup)
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                // 设置消费监听器。
                .setMessageListener(messageView -> {
                    System.out.println("===================");
//                    log.info("Consume start messageId={}", messageView.getMessageId());
//                    if (messageView.getMessageId() != null) {
////                        try {
////                            Thread.sleep(1000000);
////                        } catch (InterruptedException e) {
////                            throw new RuntimeException(e);
////                        }
//                        throw new RuntimeException("测试消费");
//                    }

                    // 处理消息并返回消费结果。
                    log.info("Consume message successfully, messageId={}, messageBody={}", messageView.getMessageId(), StandardCharsets.UTF_8.decode(messageView.getBody()));
                    return ConsumeResult.SUCCESS;
                })
                .build();
        Thread.sleep(5000000);
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
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8080;xxx:8081。
        String endpoint = "10.10.23.139:8080";

        // 消费示例：使用 SimpleConsumer 消费普通消息，主动获取消息处理并提交。
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        String topic = "TopicTest";
        FilterExpression filterExpression = new FilterExpression("messageTag", FilterExpressionType.TAG);
        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                // 设置消费者分组。
                .setConsumerGroup("YourConsumerGroup")
                // 设置接入点。
                .setClientConfiguration(ClientConfiguration.newBuilder().setEndpoints(endpoint).build())
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                // 设置从服务端接受消息的最大等待时间
                .setAwaitDuration(Duration.ofSeconds(1))
                .build();
        try {
            // SimpleConsumer 需要主动获取消息，并处理。
            List<MessageView> messageViewList = simpleConsumer.receive(10, Duration.ofSeconds(30));
            messageViewList.forEach(messageView -> {
                System.out.println(messageView);
                log.info(StandardCharsets.UTF_8.decode(messageView.getBody()).toString());
                // 消费处理完成后，需要主动调用 ACK 提交消费结果。
                try {
                    simpleConsumer.ack(messageView);
                } catch (ClientException e) {
                    log.error("Failed to ack message, messageId={}", messageView.getMessageId(), e);
                }
            });
        } catch (ClientException e) {
            // 如果遇到系统流控等原因造成拉取失败，需要重新发起获取消息请求。
            log.error("Failed to receive message", e);
        }
        Thread.sleep(5000);
    }

}

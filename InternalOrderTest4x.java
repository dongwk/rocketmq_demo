//package internal;
//
//import cn.hutool.core.thread.ThreadUtil;
//import com.cloud.common.utils.DateUtils;
//import com.cloud.oms.provincial.distribution.model.param.ProvincialDistributionWarehouseParam;
//import com.cloud.oms.provincial.distribution.service.IProvincialDistributionWarehouseService;
//import com.cloud.oms.provincial.internal.model.param.*;
//import com.cloud.oms.provincial.internal.service.IInternalTradeOrderDropShippingBizService;
//import com.cloud.oms.service.clearing.DayClearingBizService;
//import com.cloud.oms.service.provincial.internal.InternalTradeBizService;
//import com.cloud.oms.service.provincial.internal.InternalTradeWmsBizService;
//import com.cloud.rocketmq.message.wms.TransferResultMessage;
//import com.google.common.collect.Lists;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
//import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
//import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
//import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
//import org.apache.rocketmq.client.exception.MQBrokerException;
//import org.apache.rocketmq.client.exception.MQClientException;
//import org.apache.rocketmq.client.producer.DefaultMQProducer;
//import org.apache.rocketmq.client.producer.MessageQueueSelector;
//import org.apache.rocketmq.client.producer.SendResult;
//import org.apache.rocketmq.common.message.Message;
//import org.apache.rocketmq.common.message.MessageExt;
//import org.apache.rocketmq.common.message.MessageQueue;
//import org.apache.rocketmq.remoting.common.RemotingHelper;
//import org.apache.rocketmq.remoting.exception.RemotingException;
//import org.junit.jupiter.api.Test;
//
//import java.math.BigDecimal;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//
///**
// * 测试
// *
// * @author dongwk
// * @date 2024-09-12 15:01
// */
//@Slf4j
//public class InternalOrderTest4x {
//
//    //    @Autowired
//    private IProvincialDistributionWarehouseService warehouseService;
//
//    //    @Autowired
//    private IInternalTradeOrderDropShippingBizService orderBizService;
//
//    //    @Autowired
////    private MQTransactionListener mqTransactionListener;      // 事务消息监听器
//    // 消息生产者配置信息
//    private String groupName = "rocketmq_demo";                 // 集群名称，这边以应用名称作为集群名称
//    private String pNamesrvAddr = "10.10.23.139:9876";          // 生产者nameservice地址
//    private Integer maxMessageSize = 4096;                      // 消息最大大小，默认4M
//    private Integer sendMsgTimeout = 30000;                     // 消息发送超时时间，默认3秒
//    private Integer retryTimesWhenSendFailed = 2;               // 消息发送失败重试次数，默认2次
//    private static ExecutorService executor = ThreadUtil.newExecutor(32); // 执行任务的线程池
//
//    /**
//     * 普通消息生产者.
//     */
//    @Test
//    public void sendMq() throws Exception {
//        // 初始化一个producer并设置Producer group name
//        DefaultMQProducer producer = new DefaultMQProducer(this.groupName); //（1）
//        // 设置NameServer地址
//        producer.setNamesrvAddr(pNamesrvAddr);  //（2）
//        // 启动producer
//        producer.start();
//        for (int i = 0; i < 1; i++) {
//            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
//            Message msg = new Message("TopicTest" /* Topic */,
//                    "TagA" /* Tag */,
//                    ("Hello RocketMQ " + i + " " + DateUtils.getTime()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//            );   //（3）
//            // 利用producer进行发送，并同步等待发送结果
//            SendResult sendResult = producer.send(msg);   //（4）
//            System.out.printf("%s%n", sendResult);
//        }
//        // 一旦producer不再使用，关闭producer
//        producer.shutdown();
//    }
//
//    /**
//     * 普通消息消费者.
//     */
//    @Test
//    public void receiveMq() throws Exception {
//        // 初始化consumer，并设置consumer group name
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
//
//        // 设置NameServer地址
//        consumer.setNamesrvAddr(pNamesrvAddr);
//        //订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
////        consumer.subscribe("TopicTest", "*"); // 普通队列
//        consumer.subscribe("TestTopic", "*"); // 延迟队列
//        //注册回调接口来处理从Broker中收到的消息
//        consumer.registerMessageListener(new MessageListenerConcurrently() {
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
//                for (MessageExt msg : msgs) {
//                    System.out.printf("消费时间:%s %n", DateUtils.getTime());
//                    System.out.printf("消息ID:%s %n", msg.getMsgId());
//                    System.out.printf("消息Body:%s %n", new String(msg.getBody()));
//                }
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//        // 启动Consumer
//        consumer.start();
//        System.out.printf("Consumer Started.%n");
//
//        while (true) {
//            Thread.sleep(1000);
//        }
//    }
//
//    /**
//     * 顺序消息.
//     */
//    public void sendMqTransaction() throws Exception {
//        try {
//            DefaultMQProducer producer = new DefaultMQProducer(groupName);
//            producer.start();
//
//            String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
//            for (int i = 0; i < 100; i++) {
//                int orderId = i % 10;
//                Message msg =
//                        new Message("TopicTest", tags[i % tags.length], "KEY" + i,
//                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
//                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
//                    @Override
//                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
//                        Integer id = (Integer) arg;
//                        int index = id % mqs.size(); // 选择消息队列的索引，根据订单ID选择
//                        return mqs.get(index); // 返回选择的消息队列，同一队列保证顺序消息
//                    }
//                }, orderId);
//
//                System.out.printf("%s%n", sendResult);
//            }
//
//            producer.shutdown();
//        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 延时消息.
//     */
//    @Test
//    public void sendDeayMq() throws Exception {
//        // Instantiate a producer to send scheduled messages
//        DefaultMQProducer producer = new DefaultMQProducer(groupName);
//        producer.setNamesrvAddr(pNamesrvAddr);  //（2）
//        // Launch producer
//        producer.start();
//        int totalMessagesToSend = 1;
//        for (int i = 0; i < totalMessagesToSend; i++) {
//            Message message = new Message("TestTopic", ("Hello scheduled message " + i + " " + DateUtils.getTime()).getBytes());
//            // This message will be delivered to consumer 10 seconds later.
//            message.setDelayTimeLevel(3);
//
//            // Send the message
//            producer.send(message);
//        }
//
//        // Shutdown producer after use.
//        producer.shutdown();
//    }
//
//    /**
//     * 延时消息，自定义时间.
//     */
//    @Test
//    public void sendDeayMqWithSelfDefTime() throws Exception {
//    }
//}

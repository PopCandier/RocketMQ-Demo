package com.pop.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author Pop
 * @date 2019/8/29 22:38
 */
public class RocketMqConsumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer("pop_consumer_group");
        consumer.setNamesrvAddr("192.168.0.102:9876");
        //从哪里开始消费 表示如果这个消费组是第一次启动，那么从第一个开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //     后面可以是一个表达式，订阅，这下面的主题下面的具体tag，大概是
        consumer.subscribe("pop_test_topic","*");

        //注册监听 同步消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                System.out.println("Recevice Message: "+msgs);

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//签收
            }
        });
        //顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {

                MessageExt ext = msgs.get(0);

                if(ext.getReconsumeTimes()==3){//超过了三次
                    //持久化处理
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
    }

}

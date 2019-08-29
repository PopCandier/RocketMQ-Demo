package com.pop.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * @author Pop
 * @date 2019/8/29 22:37
 */
public class RocketMqProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        /**
         * 生产组，因为rocket中，生产者和消费者都可以进行集群
         */
        DefaultMQProducer producer = new DefaultMQProducer("pop_producer");
        //设置 nameserver 的地址
        producer.setNamesrvAddr("192.168.0.102:9876");//会从命名服务器上拿到broker的地址
        producer.start();//启动

        int num = 0;
        while(num<20){
            num++;
            //Topic                                         tag 算是一种路由，筛选,表示某一类
            Message message = new Message("pop_test_topic","TagA",("hello,rockmq"+num).getBytes());
            SendResult result=producer.send(message);//同步发送

            producer.sendOneway(message);

            //异步发送
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    //执行回调成功
                }

                @Override
                public void onException(Throwable e) {
                    //发生异常
                }
            });

            //指定发送策略
            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    System.out.println(mqs.size());//4个，因为之前控制台看到了
                    return mqs.get(0);//我们也可以指定落到具体0号分区,这样所有消息都落到0号分区了。
                }
            },"key-"+num);//这里的key和select 方法中的arg应该一样的，你可以通过指定具体的key，来进行取模之类的，保证落到哪个分区
            //这里可以会想起了kafka的log落到50分区的算法，key的hash和50取模

            System.out.println(result);
        }
    }
}

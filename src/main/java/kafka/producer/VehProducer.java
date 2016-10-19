/*
 * Project: alex-spark-streaming
 * 
 * File Created at 2016年9月18日
 * 
 * Copyright 2016 CMCC Corporation Limited.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * ZYHY Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license.
 */
package kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;

/**
 * @Type VehProducer.java
 * @Desc 
 * @author alex
 * @date 2016年9月18日 上午9:52:04
 * @version 
 */
public class VehProducer {
    public static final String ZK_CONNECT ="zk.connect";
    public static final String BROKER_LIST ="metadata.broker.list";
    public static final String TOPIC ="topic";
    
    
    public static void main(String[] args) {  
        Properties props = new Properties();  
        props.put("zk.connect", "192.168.6.100:2181");  
        props.put("serializer.class", "kafka.serializer.StringEncoder");  
        props.put("metadata.broker.list", "192.168.6.100:9092");  
        ProducerConfig config = new ProducerConfig(props);  
        Producer<String, String> producer = new Producer<String, String>(config);  
        for (int i = 0; i < 10000000; i++)  {
            producer.send(new KeyedMessage<String, String>("test_4", "message" + i));  
            System.out.println(" has send : message"+i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }  
    
    
    
    

}


/**
 * Revision history
 * -------------------------------------------------------------------------
 * 
 * Date Author Note
 * -------------------------------------------------------------------------
 * 2016年9月18日 alex creat
 */
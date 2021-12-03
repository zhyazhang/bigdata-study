package com.aifurion.utils;

import java.util.Properties;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 16:29
 */
public class KafkaUtil {


    public static Properties getProperties() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization" +
                ".StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization" +
                ".StringDeserializer");

        return properties;
    }
}

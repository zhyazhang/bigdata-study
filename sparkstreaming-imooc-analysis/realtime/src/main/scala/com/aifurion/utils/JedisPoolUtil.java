package com.aifurion.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/6 18:29
 */
public class JedisPoolUtil {

    //声明为volatile防止指令重排序

    private static volatile JedisPool jedisPool = null;

    private static final String HOST = "hadoop102";

    private static final int PORT = 6379;

    //双重检查锁实现懒汉式单例

    public static Jedis getConnection() {

        if (jedisPool == null) {
            synchronized (JedisPoolUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(30);
                    config.setMaxIdle(10);
                    jedisPool = new JedisPool(config, HOST, PORT);

                }
            }
        }

        return jedisPool.getResource();
    }


}

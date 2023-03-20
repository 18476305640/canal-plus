package com.alibaba.canal.utils;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @auther zzyy
 * @create 2022-12-23 12:00
 */
@Configuration
public class JedisPoolUtils
{
    //创建连接池对象
    private static JedisPool jedisPool;


    private static String HOST = "192.168.87.105";
    private static String REDIS_PWD = "3333";
    private static Integer PORT = 6479;
    private static Integer MAX_TOTAL = 50;
    private static Integer MAX_IDLE = 10;

    // 定义静态代码块 当类加载时即读取配置文件 并对连接池对象进行数值设置
    static
    {
        // 从Properties对象中获取数据 并设置到JedisPoolConfig中
        JedisPoolConfig jedisPoolConfig=new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(MAX_TOTAL);
        jedisPoolConfig.setMaxIdle(MAX_IDLE);

        // 初始化JedisPool连接池对象
        jedisPool=new JedisPool(jedisPoolConfig,HOST,PORT,3000,REDIS_PWD);
    }

    @Bean
    // 提供获取连接的方法(返回连接池对象)
    public static Jedis getJedis()
    {
        return jedisPool.getResource();
    }


}
package com.alibaba.canal.canal.listener;

import com.alibaba.canal.canal.listen.CanalListener;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.canal.canal.listen.annotation.CanalListen;
import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.Jedis;

import java.util.List;

@CanalListen("canal_test\\..*")
public class UserListener implements CanalListener {

    @Autowired
    private Jedis jedis;

    @Override
    public void create(String source,List<CanalEntry.Column> beforeRow, List<CanalEntry.Column> afterRow) {
        System.out.println("Changed! is Create!");
        System.out.println(jedis);
    }

    @Override
    public void delete(String source,List<CanalEntry.Column> beforeRow, List<CanalEntry.Column> afterRow) {
        System.out.println("Changed! is delete!");
        System.out.println(jedis);
    }

    @Override
    public void update(String source,List<CanalEntry.Column> beforeRow, List<CanalEntry.Column> afterRow) {
        System.out.println("Changed! is update!");
        System.out.println(jedis);
    }
}

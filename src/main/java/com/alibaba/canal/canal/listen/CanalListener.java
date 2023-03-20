package com.alibaba.canal.canal.listen;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.annotation.Annotation;
import java.util.List;

// 表示监听目标
public interface CanalListener {
    default void create(String source,List<CanalEntry.Column> beforeRow, List<CanalEntry.Column> afterRow){}
    default void delete(String source,List<CanalEntry.Column> beforeRow, List<CanalEntry.Column> afterRow){}
    default void update(String source,List<CanalEntry.Column> beforeRow, List<CanalEntry.Column> afterRow){}

}

package com.alibaba.canal.canal.listen;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MotionPerception {

    public static void handle(List<CanalEntry.Entry> entries, Map<String, CanalListener> listenerMap) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                //获取变更的row数据
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error,data:" + entry.toString(),e);
            }
            //获取变动类型
            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                // 筛选出哪些符合

                CanalEntry.Header header = entry.getHeader();
                String source = header.getSchemaName() + "." + header.getTableName();
                Map<String, CanalListener> targetListenerMap = canalListenerFilter(listenerMap,source);
                if (eventType == CanalEntry.EventType.INSERT) {
                    insert(source,rowData.getBeforeColumnsList(), rowData.getAfterColumnsList(),targetListenerMap);
                } else if (eventType == CanalEntry.EventType.DELETE) {
                    delete(source,rowData.getBeforeColumnsList(),rowData.getAfterColumnsList(),targetListenerMap);
                } else {//EventType.UPDATE
                    update(source,rowData.getBeforeColumnsList(),rowData.getAfterColumnsList(),targetListenerMap);
                }
            }
        }
    }

    private static void update(String source,List<CanalEntry.Column> beforeColumnsList,List<CanalEntry.Column> afterColumnsList, Map<String, CanalListener> targetListenerMap) {
        Collection<CanalListener> listenerInstances = targetListenerMap.values();
        for (CanalListener listenerInstance : listenerInstances) {
            listenerInstance.update(source,beforeColumnsList,afterColumnsList);
        }
    }

    private static void delete(String source,List<CanalEntry.Column> beforeColumnsList,List<CanalEntry.Column> afterColumnsList, Map<String, CanalListener> targetListenerMap) {
        Collection<CanalListener> listenerInstances = targetListenerMap.values();
        for (CanalListener listenerInstance : listenerInstances) {
            listenerInstance.delete(source,beforeColumnsList,afterColumnsList);
        }
    }

    private static void insert(String source,List<CanalEntry.Column> beforeColumnsList,List<CanalEntry.Column> afterColumnsList, Map<String, CanalListener> targetListenerMap) {
        Collection<CanalListener> listenerInstances = targetListenerMap.values();
        for (CanalListener listenerInstance : listenerInstances) {
            listenerInstance.create(source,beforeColumnsList,afterColumnsList);
        }
    }

    private static Map<String, CanalListener> canalListenerFilter(Map<String, CanalListener> listenerMap, String dbAndTable) {
        Map<String, CanalListener> targetListenerMap = new HashMap<>();
        listenerMap.forEach((key,value)->{
            boolean isPattern = key.indexOf("\\.") != -1;
            if (  ! isPattern && dbAndTable.trim().equals(key.trim()) )  targetListenerMap.put(key,value);
            // 要用正则匹配
            if (isPattern) {
                Pattern pattern = Pattern.compile(key);
                Matcher matcher = pattern.matcher(dbAndTable);
                if(matcher.find()) targetListenerMap.put(key,value);
            }
        });
        return targetListenerMap;
    }
}

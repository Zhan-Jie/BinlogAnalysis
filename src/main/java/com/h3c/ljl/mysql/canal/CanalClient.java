package com.h3c.ljl.mysql.canal;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class CanalClient {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    @Value("${canal.server.hostIp}")
    private String hostIP;
    @Value("${canal.server.port}")
    private int port;
    @Value("${canal.server.destination}")
    private String destination;
    @Value("${canal.server.username}")
    private String username;
    @Value("${canal.server.password}")
    private String password;

    public  void analysisBinlog() {
        // 创建链接     AddressUtils.getHostIp()    "192.168.61.132"    hadoop
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostIP, port), destination, username, password);
        int batchSize = 1;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmtryCount = 120;
            while (emptyCount < totalEmtryCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private  void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
/*            "================> binlog[mysql-bin.000005:442] , name[test,user] , eventType : INSERT\n" +
                    "name : success    update=false\n" +
                    "age : 23    update=false"*/

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList(),eventType);
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList(),eventType);
                } else {
                   /* System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList(),eventType);*/
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList(),eventType);
                }
            }
        }
    }

    private  void printColumn(List<Column> columns,EventType eventType) {
        Map<String,String> map = new HashMap<String,String>();
        for (Column column : columns) {
            //System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            //KafkaProducer.sendMsg("canal", UUID.randomUUID().toString() ,column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            System.out.println(column.getName() + " : " + column.getValue());
            map.put(column.getName(),column.getValue());
          //  kafkaTemplate.send("canal", key, column.getName() + " : " + column.getValue());
        }
        kafkaTemplate.send("canal", UUID.randomUUID().toString(), eventType + ":" + map);
    }
}

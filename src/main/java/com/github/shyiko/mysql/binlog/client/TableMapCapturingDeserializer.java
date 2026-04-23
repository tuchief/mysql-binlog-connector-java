package com.github.shyiko.mysql.binlog.client;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * TABLE_MAP 事件的"捕获代理"反序列化器。
 *
 * <p>mysql-binlog-connector-java 的 EventDeserializer 内部维护了一个
 * tableMapEventByTableId Map，用于 WriteRows/UpdateRows/DeleteRows 事件解析时
 * 查询列数量。但该 Map 是 private 的，外部无法直接访问。
 *
 * <p>此类的作用是：
 * <ol>
 *   <li>用标准的 {@link TableMapEventDataDeserializer} 解析 TABLE_MAP 事件，</li>
 *   <li>将解析结果同步写入外部传入的 sharedTableMap，</li>
 *   <li>确保压缩 Row 事件的 Deserializer 能读到最新的表结构。</li>
 * </ol>
 */
class TableMapCapturingDeserializer implements EventDataDeserializer<TableMapEventData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    private final TableMapEventDataDeserializer delegate =
            new TableMapEventDataDeserializer();
    private final Map<Long, TableMapEventData> sharedTableMap;

    TableMapCapturingDeserializer(Map<Long, TableMapEventData> sharedTableMap) {
        this.sharedTableMap = sharedTableMap;
    }

    @Override
    public TableMapEventData deserialize(ByteArrayInputStream in) throws IOException {
        TableMapEventData data = delegate.deserialize(in);
        if (data != null) {
            sharedTableMap.put(data.getTableId(), data);
            log.info("Captured TABLE_MAP: tableId=" + data.getTableId() + ", db=" + data.getDatabase() + ", table=" + data.getTable());
        }
        return data;
    }
}

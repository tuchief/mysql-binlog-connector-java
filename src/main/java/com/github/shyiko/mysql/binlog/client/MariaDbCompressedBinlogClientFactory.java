package com.github.shyiko.mysql.binlog.client;

import com.github.shyiko.mysql.binlog.deserializer.*;
import com.github.shyiko.mysql.binlog.event.MariaDbEventTypes;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * MariaDB 压缩 binlog 客户端工厂 —— 本库主入口。
 *
 * <h2>快速使用（实时 CDC）</h2>
 * <pre>{@code
 * BinaryLogClient client = new BinaryLogClient("127.0.0.1", 3306, "user", "pwd");
 * MariaDbCompressedBinlogClientFactory.configure(client);
 * client.registerEventListener(event -> { ... });
 * client.connect();
 * }</pre>
 *
 * <h2>快速使用（离线文件读）</h2>
 * <pre>{@code
 * EventDeserializer ed = MariaDbCompressedBinlogClientFactory.createEventDeserializer();
 * try (BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, ed)) {
 *     for (Event event; (event = reader.readEvent()) != null; ) { ... }
 * }
 * }</pre>
 */
public final class MariaDbCompressedBinlogClientFactory {

    private final Logger log = Logger.getLogger(getClass().getName());

    private MariaDbCompressedBinlogClientFactory() {}

    /**
     * 为 BinaryLogClient 安装 MariaDB 压缩事件支持（一行代码调用）。
     */
    public static void configure(BinaryLogClient client) {
        client.setEventDeserializer(createEventDeserializer());
    }

    /**
     * 创建注册了全部 7 种压缩事件 Deserializer 的 EventDeserializer。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static EventDeserializer createEventDeserializer() {
        Map<Long, TableMapEventData> sharedTableMap = new ConcurrentHashMap<>();

        EventDeserializer ed = new EventDeserializer();

        // TABLE_MAP 代理：同步到 sharedTableMap，供压缩 Row Deserializer 使用
        ed.setEventDataDeserializer(
                EventType.TABLE_MAP,
                new TableMapCapturingDeserializer(sharedTableMap));

        // QUERY_COMPRESSED_EVENT (165)
        ed.setEventDataDeserializer(
            EventType.byEventNumber(MariaDbEventTypes.QUERY_COMPRESSED_EVENT),
                new CompressedQueryEventDeserializer());

        // WRITE_ROWS_COMPRESSED V1(161) + non-V1(168)
        CompressedWriteRowsEventDeserializer wr = new CompressedWriteRowsEventDeserializer(sharedTableMap);
        ed.setEventDataDeserializer(EventType.byEventNumber(MariaDbEventTypes.WRITE_ROWS_COMPRESSED_EVENT_V1), wr);
        ed.setEventDataDeserializer(EventType.byEventNumber(MariaDbEventTypes.WRITE_ROWS_COMPRESSED_EVENT), wr);

        // UPDATE_ROWS_COMPRESSED V1(162) + non-V1(169)
        CompressedUpdateRowsEventDeserializer ur = new CompressedUpdateRowsEventDeserializer(sharedTableMap);
        ed.setEventDataDeserializer(EventType.byEventNumber(MariaDbEventTypes.UPDATE_ROWS_COMPRESSED_EVENT_V1), ur);
        ed.setEventDataDeserializer(EventType.byEventNumber(MariaDbEventTypes.UPDATE_ROWS_COMPRESSED_EVENT), ur);

        // DELETE_ROWS_COMPRESSED V1(163) + non-V1(170)
        CompressedDeleteRowsEventDeserializer dr = new CompressedDeleteRowsEventDeserializer(sharedTableMap);
        ed.setEventDataDeserializer(EventType.byEventNumber(MariaDbEventTypes.DELETE_ROWS_COMPRESSED_EVENT_V1), dr);
        ed.setEventDataDeserializer(EventType.byEventNumber(MariaDbEventTypes.DELETE_ROWS_COMPRESSED_EVENT), dr);

        return ed;
    }
}

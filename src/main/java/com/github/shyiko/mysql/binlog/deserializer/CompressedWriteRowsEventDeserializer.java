package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;

/**
 * WRITE_ROWS_COMPRESSED_EVENT / WRITE_ROWS_COMPRESSED_EVENT_V1 的反序列化器。
 *
 * <p>继承 {@link AbstractCompressedRowsDeserializer} 完成解压，
 * 再将重组后的字节流交给指定的 {@link EventDataDeserializer} 解析。
 *
 * <p>默认使用 mysql-binlog-connector-java 原生的 {@link WriteRowsEventDataDeserializer}，
 * 也可通过带 delegate 的构造函数传入自定义反序列化器（如 Debezium 的 RowDeserializers）
 * 以保持压缩路径与非压缩路径的列转换逻辑一致。
 */
public class CompressedWriteRowsEventDeserializer
        extends AbstractCompressedRowsDeserializer<WriteRowsEventData> {

    private final EventDataDeserializer<WriteRowsEventData> delegate;

    public CompressedWriteRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId) {
        // WRITE 只有一个 column bitmap
        super(tableMapEventByTableId, false);
        this.delegate = new WriteRowsEventDataDeserializer(tableMapEventByTableId);
    }

    /**
     * @param tableMapEventByTableId table_id → TableMapEventData 映射
     * @param delegate 用于解析解压后行数据的反序列化器（可传入 Debezium 自定义实现）
     */
    public CompressedWriteRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId,
            EventDataDeserializer<WriteRowsEventData> delegate) {
        super(tableMapEventByTableId, false);
        this.delegate = delegate;
    }

    @Override
    public WriteRowsEventData deserialize(ByteArrayInputStream in) throws IOException {
        ByteArrayInputStream decompressed = decompressBody(in);
        return delegate.deserialize(decompressed);
    }
}

package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;

/**
 * DELETE_ROWS_COMPRESSED_EVENT / DELETE_ROWS_COMPRESSED_EVENT_V1 的反序列化器。
 *
 * <p>DELETE 只有 before-image 的 column bitmap，因此 hasTwoBitmaps=false。
 *
 * <p>默认使用 mysql-binlog-connector-java 原生的 {@link DeleteRowsEventDataDeserializer}，
 * 也可通过带 delegate 的构造函数传入自定义反序列化器。
 */
public class CompressedDeleteRowsEventDeserializer
        extends AbstractCompressedRowsDeserializer<DeleteRowsEventData> {

    private final EventDataDeserializer<DeleteRowsEventData> delegate;

    public CompressedDeleteRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId) {
        // DELETE 只有一个 column bitmap
        super(tableMapEventByTableId, false);
        this.delegate = new DeleteRowsEventDataDeserializer(tableMapEventByTableId);
    }

    /**
     * @param tableMapEventByTableId table_id → TableMapEventData 映射
     * @param delegate 用于解析解压后行数据的反序列化器
     */
    public CompressedDeleteRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId,
            EventDataDeserializer<DeleteRowsEventData> delegate) {
        super(tableMapEventByTableId, false);
        this.delegate = delegate;
    }

    @Override
    public DeleteRowsEventData deserialize(ByteArrayInputStream in) throws IOException {
        ByteArrayInputStream decompressed = decompressBody(in);
        return delegate.deserialize(decompressed);
    }
}

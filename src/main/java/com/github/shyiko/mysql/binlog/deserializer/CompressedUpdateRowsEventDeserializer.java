package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.UpdateRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * UPDATE_ROWS_COMPRESSED_EVENT / UPDATE_ROWS_COMPRESSED_EVENT_V1 的反序列化器。
 *
 * <p>UPDATE 事件有 before-image + after-image 两个 column bitmap，
 * 因此 hasTwoBitmaps=true。
 *
 * <p>默认使用 mysql-binlog-connector-java 原生的 {@link UpdateRowsEventDataDeserializer}，
 * 也可通过带 delegate 的构造函数传入自定义反序列化器。
 */
public class CompressedUpdateRowsEventDeserializer
        extends AbstractCompressedRowsDeserializer<UpdateRowsEventData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    private final EventDataDeserializer<UpdateRowsEventData> delegate;

    public CompressedUpdateRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId) {
        // UPDATE 有 before + after 两个 column bitmap
        super(tableMapEventByTableId, true);
        this.delegate = new UpdateRowsEventDataDeserializer(tableMapEventByTableId);
    }

    /**
     * @param tableMapEventByTableId table_id → TableMapEventData 映射
     * @param delegate 用于解析解压后行数据的反序列化器
     */
    public CompressedUpdateRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId,
            EventDataDeserializer<UpdateRowsEventData> delegate) {
        super(tableMapEventByTableId, true);
        this.delegate = delegate;
    }

    @Override
    public UpdateRowsEventData deserialize(ByteArrayInputStream in) throws IOException {
        ByteArrayInputStream decompressed = decompressBody(in);
        return delegate.deserialize(decompressed);
    }
}

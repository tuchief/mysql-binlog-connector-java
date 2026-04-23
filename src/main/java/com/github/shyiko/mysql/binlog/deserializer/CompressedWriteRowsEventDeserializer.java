package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * WRITE_ROWS_COMPRESSED_EVENT / WRITE_ROWS_COMPRESSED_EVENT_V1 的反序列化器。
 *
 * <p>继承 {@link AbstractCompressedRowsDeserializer} 完成解压，
 * 再将重组后的字节流交给官方的 {@link WriteRowsEventDataDeserializer} 解析。
 */
public class CompressedWriteRowsEventDeserializer
        extends AbstractCompressedRowsDeserializer<WriteRowsEventData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    private final WriteRowsEventDataDeserializer delegate;

    public CompressedWriteRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId) {
        // WRITE 只有一个 column bitmap
        super(tableMapEventByTableId, false);
        this.delegate = new WriteRowsEventDataDeserializer(tableMapEventByTableId);
    }

    @Override
    public WriteRowsEventData deserialize(ByteArrayInputStream in) throws IOException {
        ByteArrayInputStream decompressed = decompressBody(in);
        return delegate.deserialize(decompressed);
    }
}

package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
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
 */
public class CompressedUpdateRowsEventDeserializer
        extends AbstractCompressedRowsDeserializer<UpdateRowsEventData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    private final UpdateRowsEventDataDeserializer delegate;

    public CompressedUpdateRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId) {
        // UPDATE 有 before + after 两个 column bitmap
        super(tableMapEventByTableId, true);
        this.delegate = new UpdateRowsEventDataDeserializer(tableMapEventByTableId);
    }

    @Override
    public UpdateRowsEventData deserialize(ByteArrayInputStream in) throws IOException {
        log.info(">>> CompressedUpdateRowsEventDeserializer.deserialize() called");
        ByteArrayInputStream decompressed = decompressBody(in);
        return delegate.deserialize(decompressed);
    }
}

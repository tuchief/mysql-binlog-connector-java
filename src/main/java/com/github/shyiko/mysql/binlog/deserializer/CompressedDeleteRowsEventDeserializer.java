package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * DELETE_ROWS_COMPRESSED_EVENT / DELETE_ROWS_COMPRESSED_EVENT_V1 的反序列化器。
 *
 * <p>DELETE 只有 before-image 的 column bitmap，因此 hasTwoBitmaps=false。
 */
public class CompressedDeleteRowsEventDeserializer
        extends AbstractCompressedRowsDeserializer<DeleteRowsEventData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    private final DeleteRowsEventDataDeserializer delegate;

    public CompressedDeleteRowsEventDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId) {
        // DELETE 只有一个 column bitmap
        super(tableMapEventByTableId, false);
        this.delegate = new DeleteRowsEventDataDeserializer(tableMapEventByTableId);
    }

    @Override
    public DeleteRowsEventData deserialize(ByteArrayInputStream in) throws IOException {
        log.info(">>> CompressedDeleteRowsEventDeserializer.deserialize() called");
        ByteArrayInputStream decompressed = decompressBody(in);
        return delegate.deserialize(decompressed);
    }
}

package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.util.MariaDbDecompressor;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

/**
 * MariaDB 压缩 ROW 事件反序列化器的抽象基类。
 *
 * <h2>*_ROWS_COMPRESSED_EVENT 事件 body 结构</h2>
 * <p>压缩 row 事件的结构与普通 row 事件完全一致，只是最后的 rows data 部分
 * 被 zlib 压缩。结构如下：
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │ [post-header]                                                        │
 * │   table_id       : 6 字节 (uint48 LE)                               │
 * │   flags          : 2 字节 (uint16 LE)                               │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ [extra data]                                                         │
 * │   extra_data_len : 2 字节 (uint16 LE，包含自身2字节)                 │
 * │   extra_data     : extra_data_len - 2 字节                          │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ [body]                                                               │
 * │   column_count   : pack_len 编码（1 或 3 字节）                      │
 * │   columns_bitmap : (column_count+7)/8 字节（WRITE事件1个，UPDATE/    │
 * │                    DELETE各1个共2个）                                 │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ [压缩 rows data: 剩余所有字节]                                        │
 * │   使用 MariaDbDecompressor.decompress() 解压                         │
 * └──────────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>解压后将整个 body（column_count + bitmap + decompressed rows）重新
 * 拼接成标准 row event body，再调用父类的 deserialize() 完成解析。
 *
 * <p>参考：MariaDB 源码 sql/log_event.cc row_log_event_uncompress() (MDEV-11065)
 *
 * @param <T> 具体的 EventData 类型（WriteRowsEventData / UpdateRowsEventData / DeleteRowsEventData）
 */
abstract class AbstractCompressedRowsDeserializer<T extends EventData>
    extends AbstractRowsEventDataDeserializer<T> {

    private final Logger log = Logger.getLogger(getClass().getName());

    /**
     * UPDATE 事件有两个 bitmap（before + after），其他事件只有一个。
     */
    private final boolean hasTwoBitmaps;

    AbstractCompressedRowsDeserializer(
        Map<Long, TableMapEventData> tableMapEventByTableId,
        boolean hasTwoBitmaps) {
        super(tableMapEventByTableId);
        this.hasTwoBitmaps = hasTwoBitmaps;
    }

    /**
     * 将压缩的 row event body 解压后，构造标准格式的 ByteArrayInputStream，
     * 交给子类的具体 deserialize 实现处理。
     *
     * @param in 原始压缩事件的 body 字节流（不含 binlog 通用头部）
     * @return 重建后的、内容为未压缩标准格式的 ByteArrayInputStream
     * @throws IOException 解析或解压失败
     */
    protected ByteArrayInputStream decompressBody(ByteArrayInputStream in)
        throws IOException {

        int totalBodyBytes = in.available();
        byte[] body = in.read(totalBodyBytes);
        int pos = 0;

        // ---- 1. table_id (6 bytes) ----
        byte[] tableIdBuf = Arrays.copyOfRange(body, pos, pos + 6);
        pos += 6;
        long tableId = 0;
        for (int i = 0; i < 6; i++) {
            tableId |= (tableIdBuf[i] & 0xFFL) << (8 * i);
        }

        // ---- 2. flags (2 bytes, little-endian) ----
        int flags = (body[pos] & 0xFF) | ((body[pos + 1] & 0xFF) << 8);
        pos += 2;

        // ---- 注意：V1 压缩事件没有 extra_data 字段 ----
        // extra_data 只存在于 MySQL 5.6+ 的 version-2 rows event 中
        // MariaDB ROWS_COMPRESSED_EVENT_V1 结构：
        //   table_id(6) + flags(2) + column_count + bitmap(s) + 压缩数据
        // 无需任何 extra_data 的读取或判断

        // ---- 3. column_count (pack_len 编码) ----
        int firstByte = body[pos++] & 0xFF;
        byte[] columnCountBytes;
        int columnCount;
        if (firstByte < 251) {
            columnCount = firstByte;
            columnCountBytes = new byte[]{(byte) firstByte};
        } else if (firstByte == 252) {
            int lo = body[pos++] & 0xFF;
            int hi = body[pos++] & 0xFF;
            columnCount = lo | (hi << 8);
            columnCountBytes = new byte[]{(byte) 252, (byte) lo, (byte) hi};
        } else if (firstByte == 253) {
            int b1 = body[pos++] & 0xFF;
            int b2 = body[pos++] & 0xFF;
            int b3 = body[pos++] & 0xFF;
            columnCount = b1 | (b2 << 8) | (b3 << 16);
            columnCountBytes = new byte[]{(byte) 253, (byte) b1, (byte) b2, (byte) b3};
        } else {
            throw new IOException(
                "Unexpected pack_len prefix byte: 0x" + Integer.toHexString(firstByte));
        }

        // ---- 4. column bitmap(s) ----
        int bitmapLen = (columnCount + 7) / 8;
        int totalBitmapBytes = bitmapLen * (hasTwoBitmaps ? 2 : 1);
        byte[] bitmaps = Arrays.copyOfRange(body, pos, pos + totalBitmapBytes);
        pos += totalBitmapBytes;

        // ---- 5. 剩余全部字节 = MariaDB 压缩头 + zlib 数据 ----
        int compressedLen = totalBodyBytes - pos;
        byte[] compressedPayload = Arrays.copyOfRange(body, pos, pos + compressedLen);

        // ---- 6. 解压 ----
        byte[] decompressedRows;
        try {
            decompressedRows = MariaDbDecompressor.decompress(compressedPayload);
        } catch (IOException e) {
            throw e;
        }

        // ---- 7. 重组标准 V1 rows event body ----
        int headerLen = 6 + 2 + columnCountBytes.length + totalBitmapBytes;
        byte[] reconstructed = new byte[headerLen + decompressedRows.length];
        int wpos = 0;
        System.arraycopy(tableIdBuf, 0, reconstructed, wpos, 6);
        wpos += 6;
        reconstructed[wpos++] = (byte) (flags & 0xFF);
        reconstructed[wpos++] = (byte) ((flags >> 8) & 0xFF);
        System.arraycopy(columnCountBytes, 0, reconstructed, wpos, columnCountBytes.length);
        wpos += columnCountBytes.length;
        System.arraycopy(bitmaps, 0, reconstructed, wpos, totalBitmapBytes);
        wpos += totalBitmapBytes;
        System.arraycopy(decompressedRows, 0, reconstructed, wpos, decompressedRows.length);

        return new ByteArrayInputStream(reconstructed);
    }
}

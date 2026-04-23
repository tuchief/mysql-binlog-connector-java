package com.github.shyiko.mysql.binlog.deserializer;

import com.github.shyiko.mysql.binlog.util.MariaDbDecompressor;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * QUERY_COMPRESSED_EVENT (type=165) 的反序列化器。
 *
 * <h2>QUERY_COMPRESSED_EVENT 事件 body 结构</h2>
 * <p>与标准 QUERY_EVENT 完全相同，只是 SQL 文本部分被 zlib 压缩：
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │ [post-header, 固定 13 字节]                                          │
 * │   thread_id      : 4 字节 (uint32 LE)                               │
 * │   exec_time      : 4 字节 (uint32 LE，执行耗时秒)                   │
 * │   db_len         : 1 字节 (uint8，数据库名长度，不含 null 终止符)    │
 * │   error_code     : 2 字节 (uint16 LE)                               │
 * │   status_vars_len: 2 字节 (uint16 LE)                               │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ [status_vars: status_vars_len 字节]                                  │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ [db: db_len 字节 + 1 字节 null 终止符]                               │
 * ├──────────────────────────────────────────────────────────────────────┤
 * │ [压缩 SQL payload: 剩余所有字节]                                      │
 * │   使用 MariaDbDecompressor.decompress() 解压得到 SQL 文本            │
 * └──────────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>参考：MariaDB 源码 sql/log_event.cc query_event_uncompress() (MDEV-11065)
 */
public class CompressedQueryEventDeserializer
        implements EventDataDeserializer<QueryEventData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    /** Query event post-header 固定长度 = 13 字节 */
    private static final int QUERY_POST_HEADER_LEN = 13;
    /** post-header 中 db_len 字段的偏移 */
    private static final int Q_DB_LEN_OFFSET = 8;
    /** post-header 中 status_vars_len 字段的偏移 */
    private static final int Q_STATUS_VARS_LEN_OFFSET = 11;

    @Override
    public QueryEventData deserialize(ByteArrayInputStream in) throws IOException {
        // ---- 读取 post-header（13 字节）----
        byte[] postHeader = new byte[QUERY_POST_HEADER_LEN];
        in.read(postHeader);

        long threadId  = readUInt32LE(postHeader, 0);
        long execTime  = readUInt32LE(postHeader, 4);
        int  dbLen     = postHeader[Q_DB_LEN_OFFSET] & 0xFF;
        int  errorCode = readUInt16LE(postHeader, 9);
        int  statusLen = readUInt16LE(postHeader, Q_STATUS_VARS_LEN_OFFSET);

        // ---- 跳过 status_vars ----
        if (statusLen > 0) {
            in.skip(statusLen);
        }

        // ---- 读取 db（dbLen + null 终止符）----
        byte[] dbBytes = new byte[dbLen];
        if (dbLen > 0) {
            in.read(dbBytes);
        }
        in.skip(1); // null terminator

        // ---- 读取压缩 SQL payload（剩余所有字节）----
        int remaining = in.available();
        byte[] compressedSql = new byte[remaining];
        in.read(compressedSql);

        // ---- zlib 解压 ----
        byte[] sqlBytes;
        try {
            sqlBytes = MariaDbDecompressor.decompress(compressedSql);
        } catch (IOException e) {
            throw e;
        }

        // ---- 组装 QueryEventData ----
        QueryEventData data = new QueryEventData();
        data.setThreadId(threadId);
        data.setExecutionTime(execTime);
        data.setErrorCode(errorCode);
        data.setDatabase(new String(dbBytes, StandardCharsets.UTF_8));
        data.setSql(new String(sqlBytes, StandardCharsets.UTF_8));

        return data;
    }

    // -----------------------------------------------------------------------
    // 小工具
    // -----------------------------------------------------------------------

    private static long readUInt32LE(byte[] buf, int off) {
        return  (buf[off    ] & 0xFFL)
             | ((buf[off + 1] & 0xFFL) << 8)
             | ((buf[off + 2] & 0xFFL) << 16)
             | ((buf[off + 3] & 0xFFL) << 24);
    }

    private static int readUInt16LE(byte[] buf, int off) {
        return (buf[off] & 0xFF) | ((buf[off + 1] & 0xFF) << 8);
    }
}

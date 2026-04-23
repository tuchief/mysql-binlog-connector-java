package com.github.shyiko.mysql.binlog.event;

/**
 * MariaDB 压缩 binlog 事件类型码。
 *
 * <p>MariaDB 10.2+ 在开启 log_bin_compress=ON 时，会将超过
 * log_bin_compress_min_len 字节的事件用 zlib 压缩后以这些类型写入 binlog。
 * 标准 slave I/O 线程在写 relay log 前会自动解压，但第三方 Java 客户端
 * 不会，需要手动注册对应的 EventDataDeserializer。
 *
 * <p>类型码来源：MariaDB 源码 sql/log_event.h (10.2+)
 * <pre>
 *   QUERY_COMPRESSED_EVENT          = 165
 *   WRITE_ROWS_COMPRESSED_EVENT_V1  = 161
 *   UPDATE_ROWS_COMPRESSED_EVENT_V1 = 162
 *   DELETE_ROWS_COMPRESSED_EVENT_V1 = 163
 *   WRITE_ROWS_COMPRESSED_EVENT     = 168
 *   UPDATE_ROWS_COMPRESSED_EVENT    = 169
 *   DELETE_ROWS_COMPRESSED_EVENT    = 170
 * </pre>
 *
 * <p>V1 版本用于 MariaDB 10.2，非 V1 版本用于 10.3+（post-header 长度有变化）。
 * 实际上目前生产环境中绝大多数是 V1，两者解压逻辑完全相同，差异仅在
 * EventType 的数字码，注册时需要两组都注册。
 */
public final class MariaDbEventTypes {

    private MariaDbEventTypes() {}

    // -----------------------------------------------------------------------
    // Statement-based (SBR) 压缩事件
    // -----------------------------------------------------------------------
    /** QUERY_COMPRESSED_EVENT = 165 (0xA5) */
    public static final int QUERY_COMPRESSED_EVENT = 165;

    // -----------------------------------------------------------------------
    // Row-based (RBR) 压缩事件 — V1 版本 (MariaDB 10.2)
    // -----------------------------------------------------------------------
    /** WRITE_ROWS_COMPRESSED_EVENT_V1 = 161 */
    public static final int WRITE_ROWS_COMPRESSED_EVENT_V1 = 166;
    /** UPDATE_ROWS_COMPRESSED_EVENT_V1 = 162 */
    public static final int UPDATE_ROWS_COMPRESSED_EVENT_V1 = 167;
    /** DELETE_ROWS_COMPRESSED_EVENT_V1 = 163 */
    public static final int DELETE_ROWS_COMPRESSED_EVENT_V1 = 168;

    // -----------------------------------------------------------------------
    // Row-based (RBR) 压缩事件 — 非 V1 版本 (MariaDB 10.3+)
    // -----------------------------------------------------------------------
    /** WRITE_ROWS_COMPRESSED_EVENT = 168 */
    public static final int WRITE_ROWS_COMPRESSED_EVENT = 169;
    /** UPDATE_ROWS_COMPRESSED_EVENT = 169 */
    public static final int UPDATE_ROWS_COMPRESSED_EVENT = 170;
    /** DELETE_ROWS_COMPRESSED_EVENT = 170 */
    public static final int DELETE_ROWS_COMPRESSED_EVENT = 171;

    /**
     * 判断给定 EventType 的数值是否为 MariaDB 压缩事件。
     *
     * @param eventTypeCode EventType.getEventNumber() 的返回值
     * @return true 表示是压缩事件
     */
    public static boolean isCompressedEvent(int eventTypeCode) {
        return eventTypeCode == QUERY_COMPRESSED_EVENT
                || eventTypeCode == WRITE_ROWS_COMPRESSED_EVENT_V1
                || eventTypeCode == UPDATE_ROWS_COMPRESSED_EVENT_V1
                || eventTypeCode == DELETE_ROWS_COMPRESSED_EVENT_V1
                || eventTypeCode == WRITE_ROWS_COMPRESSED_EVENT
                || eventTypeCode == UPDATE_ROWS_COMPRESSED_EVENT
                || eventTypeCode == DELETE_ROWS_COMPRESSED_EVENT;
    }
}

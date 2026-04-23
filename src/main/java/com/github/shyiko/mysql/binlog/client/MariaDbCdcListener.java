package com.github.shyiko.mysql.binlog.client;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * 生产级 MariaDB CDC（Change Data Capture）监听器示例。
 *
 * <p>演示如何使用本库监听 MariaDB 压缩 binlog，捕获 INSERT/UPDATE/DELETE/DDL 变更。
 *
 * <h2>MariaDB 权限要求</h2>
 * <pre>
 * GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
 * GRANT SELECT ON your_db.* TO 'cdc_user'@'%';
 * FLUSH PRIVILEGES;
 * </pre>
 *
 * <h2>MariaDB my.cnf 配置要求</h2>
 * <pre>
 * [mysqld]
 * server-id         = 1
 * log_bin           = ON
 * binlog_format     = ROW          # 必须是 ROW 模式才有行变更事件
 * binlog_row_image  = FULL         # 记录完整行数据（推荐）
 * log_bin_compress  = ON           # 启用压缩（本库正是为此设计）
 * </pre>
 */
public class MariaDbCdcListener {

    private final Logger log = Logger.getLogger(getClass().getName());

    // -----------------------------------------------------------------------
    // 配置
    // -----------------------------------------------------------------------
    private final String  host;
    private final int     port;
    private final String  username;
    private final String  password;
    private final long    serverId;   // 必须与 MariaDB 集群中其他节点不同

    // -----------------------------------------------------------------------
    // 运行时状态
    // -----------------------------------------------------------------------
    private BinaryLogClient client;

    /** tableId -> (db.table) 映射，由 TABLE_MAP 事件填充 */
    private final Map<Long, String> tableIdToFullName = new ConcurrentHashMap<>();
    /** tableId -> TableMapEventData，列类型信息 */
    private final Map<Long, TableMapEventData> tableMapCache = new ConcurrentHashMap<>();

    /** 统计计数 */
    private final AtomicLong insertCount = new AtomicLong();
    private final AtomicLong updateCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();
    private final AtomicLong queryCount  = new AtomicLong();

    public MariaDbCdcListener(String host, int port,
                               String username, String password,
                               long serverId) {
        this.host     = host;
        this.port     = port;
        this.username = username;
        this.password = password;
        this.serverId = serverId;
    }

    // -----------------------------------------------------------------------
    // 启动 / 停止
    // -----------------------------------------------------------------------

    /**
     * 启动监听（异步，在后台线程运行）。
     *
     * @param connectTimeoutMs 连接超时毫秒数
     * @throws IOException 连接失败
     */
    public void start(long connectTimeoutMs) throws IOException, TimeoutException {
        client = new BinaryLogClient(host, port, username, password);
        client.setServerId(serverId);

        // ===== 核心：安装 MariaDB 压缩事件支持 =====
        // MariaDbCompressedBinlogClientFactory.configure(client);

        // 注册事件监听器
        client.registerEventListener(this::onEvent);

        // 注册生命周期监听
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient c) {
                log.info("Connected to MariaDB binlog stream: "+host+":"+port+", binlog="+c.getBinlogFilename()+":"+c.getBinlogPosition());
            }
            @Override
            public void onDisconnect(BinaryLogClient c) {
                log.warning("Disconnected from MariaDB binlog stream");
            }
            @Override
            public void onEventDeserializationFailure(BinaryLogClient c, Exception ex) {
                log.info("Event deserialization failure (will skip event): " + ex.getMessage());
            }
            @Override
            public void onCommunicationFailure(BinaryLogClient c, Exception ex) {
                log.info("Communication failure: " + ex.getMessage());
            }
        });

        // 异步连接（在新线程中持续监听）
        client.connect(connectTimeoutMs);
        log.info("CDC listener started (serverId=" + serverId + ", host=" + host + ":" + port + ")");
    }

    /**
     * 停止监听并断开连接。
     */
    public void stop() throws IOException {
        if (client != null) {
            client.disconnect();
            log.info("CDC listener stopped. Stats: insert=" + insertCount.get() + ", update=" + updateCount.get() + ", delete=" + deleteCount.get() + ", query=" + queryCount.get());
        }
    }

    // -----------------------------------------------------------------------
    // 事件处理
    // -----------------------------------------------------------------------

    private void onEvent(Event event) {
        EventData data = event.getData();

        if (data instanceof TableMapEventData) {
            // 缓存表信息（由 TableMapCapturingDeserializer 同步到 sharedTableMap，
            // 这里再额外维护 tableId -> db.table 映射）
            TableMapEventData tme = (TableMapEventData) data;
            tableMapCache.put(tme.getTableId(), tme);
            tableIdToFullName.put(tme.getTableId(),
                    tme.getDatabase() + "." + tme.getTable());

        } else if (data instanceof WriteRowsEventData) {
            handleInsert((WriteRowsEventData) data);

        } else if (data instanceof UpdateRowsEventData) {
            handleUpdate((UpdateRowsEventData) data);

        } else if (data instanceof DeleteRowsEventData) {
            handleDelete((DeleteRowsEventData) data);

        } else if (data instanceof QueryEventData) {
            handleQuery(event, (QueryEventData) data);

        } else if (data instanceof XidEventData) {
            log.warning("Transaction committed (xid=" + ((XidEventData) data).getXid() + ")");
        }
    }

    private void handleInsert(WriteRowsEventData data) {
        insertCount.incrementAndGet();
        String table = tableIdToFullName.getOrDefault(data.getTableId(), "unknown");
        List<Serializable[]> rows = data.getRows();

        log.info("[INSERT] table=" + table + ", rowCount=" + rows.size());
        for (Serializable[] row : rows) {
            log.info("  INSERT row: " + formatRow(row));
        }
    }

    private void handleUpdate(UpdateRowsEventData data) {
        updateCount.incrementAndGet();
        String table = tableIdToFullName.getOrDefault(data.getTableId(), "unknown");
        List<Map.Entry<Serializable[], Serializable[]>> rows = data.getRows();

        log.info("[UPDATE] table=" + table + ", rowCount=" + rows.size());
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            log.info("  UPDATE before: " + formatRow(row.getKey()));
            log.info("  UPDATE after:  " + formatRow(row.getValue()));
        }
    }

    private void handleDelete(DeleteRowsEventData data) {
        deleteCount.incrementAndGet();
        String table = tableIdToFullName.getOrDefault(data.getTableId(), "unknown");
        List<Serializable[]> rows = data.getRows();

        log.info("[DELETE] table=" + table + ", rowCount=" + rows.size());
        for (Serializable[] row : rows) {
            log.info("  DELETE row: " + formatRow(row));
        }
    }

    private void handleQuery(Event event, QueryEventData data) {
        queryCount.incrementAndGet();
        String sql = data.getSql();

        // 过滤掉高频无意义的 BEGIN
        if ("BEGIN".equalsIgnoreCase(sql.trim())) {
            return;
        }
        log.info("[QUERY] db=" + data.getDatabase() + ", sql=" + sql);
    }

    // -----------------------------------------------------------------------
    // 工具方法
    // -----------------------------------------------------------------------

    private static String formatRow(Serializable[] row) {
        if (row == null) return "null";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < row.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(row[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // 主方法（演示用）
    // -----------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        MariaDbCdcListener listener = new MariaDbCdcListener(
                "10.168.207.65",    // host
                33506,           // port
                "root",     // username
                "rootpwd",     // password
                1688L           // serverId（需唯一）
        );

        // 添加 JVM 关闭钩子，优雅停止
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                listener.stop();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        // 启动，超时 10 秒
        listener.start(TimeUnit.SECONDS.toMillis(10));

        // 保持主线程存活（生产中通常是 Spring 容器管理，不需要此循环）
        Thread.currentThread().join();
    }
}

package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.regex.Pattern;

import static com.github.shyiko.mysql.binlog.event.EventType.*;

public class Test {

    public static void main(String[] args) throws IOException {
        BinaryLogClient client = new BinaryLogClient("192.168.215.2", 3306, "root", "dataknown1234");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
            EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                EventType eventType = event.getHeader().getEventType();
                String dbTable = null;
                Integer currentDummyValue = null;
                if (eventType == EventType.TABLE_MAP) {
                    TableMapEventData data = event.getData();
                    String db = data.getDatabase();
                    String table = data.getTable();
                    dbTable = db + "-" + table;
                }
                if (eventType == EventType.QUERY) {
                    QueryEventData queryEventData = event.getData();

                    byte[] statusVars = queryEventData.getStatusVars();
                    // int lcTimeNames = StatusVarsParser.parseLcTimeNames(statusVars);
                    // System.out.println("解析到 lc_time_names = " + lcTimeNames);

                    int lcTimeNames = parseLcTimeNames(statusVars);
                    System.out.println("解析到 lc_time_names = " + lcTimeNames);

                    String sql = queryEventData.getSql();
                    if (sql != null) {
                        // 检查 SET @dummy= 格式
                        if (sql.matches("(?i).*SET\\s+@dummy\\s*=\\s*(\\d+).*")) {
                            // 提取 dummy 值
                            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("SET\\s+@dummy\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
                            java.util.regex.Matcher matcher = pattern.matcher(sql);
                            if (matcher.find()) {
                                currentDummyValue = Integer.parseInt(matcher.group(1));
                                System.out.println(">>> 设置 dummy 值为: " + currentDummyValue);
                            }
                        }
                        // 检查其他可能的格式
                        else if (sql.contains("/*!80000 SET @dummy=")) {
                            // 原有的注释格式处理
                            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("/\\*!80000 SET @dummy=(\\d+)");
                            java.util.regex.Matcher matcher = pattern.matcher(sql);
                            if (matcher.find()) {
                                currentDummyValue = Integer.parseInt(matcher.group(1));
                                System.out.println(">>> 设置 dummy 值为: " + currentDummyValue);
                            }
                        }
                        // 打印所有 QUERY 事件以便调试
                        System.out.println(">>> QUERY 事件: " + sql);
                    }
                }
                System.out.println("===> eventType: " + eventType +" \n===> data: " + event.getData());
                if(isWrite(eventType) || isUpdate(eventType) || isDelete(eventType)) {
                    if (isWrite(eventType)) {
                        WriteRowsEventData data = event.getData();
                        for (Serializable[] row : data.getRows()) {
                            System.out.println(row);
                        }
                    }
                    if (isUpdate(eventType)) {
                        UpdateRowsEventData data = event.getData();
                        for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                            System.out.println(row);
                        }
                    }
                    if (isDelete(eventType)) {
                        DeleteRowsEventData data = event.getData();
                        for (Serializable[] row : data.getRows()) {
                            System.out.println(row);
                        }
                    }
                }

            }
        });
        client.connect();
    }

    /**
     * 解析 statusVars 中的 lc_time_names 值
     * @param statusVars QueryEventData 中的 statusVars 字节数组
     * @return lc_time_names 的值，如果未找到返回 -1
     */
    private static int parseLcTimeNames(byte[] statusVars) {
        int index = 0;
        while (index < statusVars.length) {
            // 读取变量类型
            int varType = statusVars[index] & 0xFF;
            index++;

            switch (varType) {
                case 0:  // Q_FLAGS2_CODE
                    index += 4;
                    break;

                case 1:  // Q_SQL_MODE_CODE
                    index += 8;
                    break;

                case 2:  // Q_CATALOG_CODE
                    index += 1 + (statusVars[index] & 0xFF);
                    break;

                case 3:  // Q_AUTO_INCREMENT
                    index += 2 * 2;
                    break;

                case 4:  // Q_CHARSET_CODE
                    index += 3 * 2;
                    break;

                case 5:  // Q_TIME_ZONE_CODE
                    int timeZoneLen = statusVars[index] & 0xFF;
                    index += 1 + timeZoneLen;
                    break;

                case 6:  // Q_CATALOG_NZ_CODE
                    int catalogLen = statusVars[index] & 0xFF;
                    index += 1 + catalogLen;
                    break;

                case 7:  // Q_LC_TIME_NAMES_CODE
                    int lcTimeNames = (statusVars[index] & 0xFF) |
                        ((statusVars[index + 1] & 0xFF) << 8);
                    return lcTimeNames;

                case 8:  // Q_CHARSET_DATABASE_CODE
                    index += 2;
                    break;

                case 9:  // Q_TABLE_MAP_FOR_UPDATE_CODE
                    index += 8;
                    break;

                case 10: // Q_MASTER_DATA_WRITTEN_CODE
                    index += 4;
                    break;

                case 11: // Q_INVOKER
                    int userLen = statusVars[index] & 0xFF;
                    index += 1 + userLen;
                    int hostLen = statusVars[index] & 0xFF;
                    index += 1 + hostLen;
                    break;

                case 12: // Q_UPDATED_DB_NAMES
                    int numDbs = statusVars[index] & 0xFF;
                    index++;
                    for (int i = 0; i < numDbs; i++) {
                        int dbLen = statusVars[index] & 0xFF;
                        index += 1 + dbLen;
                    }
                    break;

                case 13: // Q_MICROSECONDS
                    index += 3;
                    break;

                default:
                    // 遇到未知类型，无法继续正确解析
                    return -1;
            }

            // 检查是否还有足够的字节可读
            if (index >= statusVars.length) {
                break;
            }
        }
        return -1; // 未找到 Q_LC_TIME_NAMES
    }
}

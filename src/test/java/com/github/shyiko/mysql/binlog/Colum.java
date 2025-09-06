package com.github.shyiko.mysql.binlog;

import lombok.Data;

@Data
public class Colum {
    public int inx;
    public String colName; // 列名
    public String dataType; // 类型
    public String schema; // 数据库
    public String table; // 表

    public Colum(String schema, String table, int idx, String colName, String dataType) {
        this.schema = schema;
        this.table = table;
        this.colName = colName;
        this.dataType = dataType;
        this.inx = idx;
    }
}

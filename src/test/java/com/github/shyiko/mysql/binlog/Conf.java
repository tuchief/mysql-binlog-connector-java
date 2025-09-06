package com.github.shyiko.mysql.binlog;

import lombok.Data;

@Data
public class Conf {
    private String host;
    private int port;
    private String username;
    private String passwd;
}

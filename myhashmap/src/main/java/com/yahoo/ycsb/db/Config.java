package com.yahoo.ycsb.db;

/**
 * Created by gomes on 06/04/15.
 */
public class Config {

    public static final String dbUrl = "jdbc:mysql://eu-cdbr-west-01.cleardb.com/heroku_1d2fd4f4e2a5f5a";
    public static final String dbClass = "com.mysql.jdbc.Driver";
    public static final String username = "b2110711a23557";
    public static final String password = "f8a4b880";


    /** The URL to connect to the database. */
    public static final String SERVER_IP = "db.ip";
    public static final String SERVER_IP_DEFAULT = "localhost";

    /** The user name to use to connect to the database. */
    public static final String SERVER_PORT = "db.port";
    public static final String SERVER_PORT_DEFAULT = "9090";
}

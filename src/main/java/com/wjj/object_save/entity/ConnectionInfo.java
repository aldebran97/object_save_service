package com.wjj.object_save.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mysql.cj.jdbc.JdbcConnection;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * mysql连接信息
 *
 * @author aldebran
 * @since 2021-08-19
 */
public class ConnectionInfo {

    public String userName;

    public String password;

    public int port;

    public String dbName;

    public String host;

    @JsonIgnore
    private volatile ConnectionPool connectionPool;

    /**
     * 直接打开连接，外界调用不属于连接池
     *
     * @return
     * @throws SQLException
     */
    public JdbcConnection openConnection() throws SQLException {
        JdbcConnection connection =
                (JdbcConnection) DriverManager.getConnection(
                        String.format("jdbc:mysql://%s:%s/%s", host, port, dbName),
                        userName, password);

        return connection;
    }

    /**
     * 初始化连接池
     *
     * @param max_connection 最大连接数
     */
    public void initConnectionPool(int max_connection) {
        if (connectionPool == null) {
            connectionPool = new ConnectionPool(this, max_connection);
        }
    }

    /**
     * 从连接池中取数据库连接
     *
     * @return
     * @throws SQLException
     */
    public PoolConnection acquireConnectionFromPool() throws SQLException {
        return connectionPool.getConnection();
    }

    /**
     * 释放连接池
     *
     * @throws SQLException
     */
    public void releasePool() throws SQLException {
        if (connectionPool != null) {
            connectionPool.close();
            connectionPool = null;
        }
    }

    @Override
    public String toString() {
        return "ConnectionInfo{" +
                "userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", port=" + port +
                ", dbName='" + dbName + '\'' +
                ", host='" + host + '\'' +
                '}';
    }
}

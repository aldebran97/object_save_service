package com.wjj.object_save.entity;

import com.mysql.cj.jdbc.JdbcConnection;

import java.sql.SQLException;

/**
 * 池化的JDBC连接
 *
 * @author aldebran
 * @since 2021-08-19
 */
public class PoolConnection {

    private ConnectionPool connectionPool;

    volatile JdbcConnection jdbcConnection;

    volatile boolean giveBack;

    volatile boolean close;

    public PoolConnection(ConnectionPool connectionPool, JdbcConnection jdbcConnection) {
        this.connectionPool = connectionPool;
        this.jdbcConnection = jdbcConnection;
    }

    public JdbcConnection getJdbcConnection() {
        if (close) {
            return null;
        }
        return jdbcConnection;
    }

    /**
     * 归还连接池
     */
    public void giveBack() {
        if (!giveBack) {
            synchronized (connectionPool) {
                connectionPool.giveBackList.add(this);
                connectionPool.semaphore.release();
                giveBack = true;
            }
        }
    }

    /**
     * 关闭连接
     *
     * @throws SQLException
     */
    void close() throws SQLException {
        if (!close) {
            jdbcConnection.close();
            jdbcConnection = null;
            close = true;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}

package com.wjj.object_save.entity;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * JDBC连接池
 * 基于信号量实现
 *
 * @author aldebran
 * @since 2021-08-19
 */
public class ConnectionPool {

    private ConnectionInfo connectionInfo;

    private final int max_connection;

    List<PoolConnection> giveBackList = new ArrayList<>();

    List<PoolConnection> allocList = new ArrayList<>();

    Semaphore semaphore;

    volatile boolean close = false;

    /**
     * 构造方法
     *
     * @param connectionInfo 连接基本信息
     * @param max_connection 最大连接数
     */
    public ConnectionPool(ConnectionInfo connectionInfo, int max_connection) {
        this.max_connection = max_connection;
        this.connectionInfo = connectionInfo;
        semaphore = new Semaphore(max_connection);
    }

    /**
     * 获取池化的JDBC连接
     *
     * @return
     */
    public PoolConnection getConnection() throws SQLException {
        if (close) {
            return null;
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("fail to acquire semaphore!", e);
        }
        synchronized (this) {
            if (giveBackList.isEmpty()) {
                PoolConnection poolConnection = new PoolConnection(this, connectionInfo.openConnection());
                allocList.add(poolConnection);
                return poolConnection;
            } else {
                PoolConnection poolConnection = giveBackList.remove(giveBackList.size() - 1);
                poolConnection.giveBack = false;
                return poolConnection;
            }
        }
    }

    /**
     * 关闭池中所有连接，并且池不能再使用
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        if (!close) {
            for (PoolConnection poolConnection : allocList) {
                poolConnection.close();
            }
            close = true;
        }
    }

}

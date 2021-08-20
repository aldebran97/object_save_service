package com.wjj.object_save.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.wjj.object_save.mapper.DataMapper;
import com.wjj.object_save.util.InputStreamUtil;
import com.wjj.object_save.util.OutputStreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;

/**
 * 哈希环，不限制传输和存储方式
 * 散列值是为32位地址空间，节点对应的散列值尽可能均匀分配
 *
 * @author aldebran
 * @since 2021-08-16
 */
public class HashRing {

    // 无符号型int最大值 2^32-1
    public static final long max_hash = 0x0ffffffffL;

    // 池内最大连接数，可自定义
    public static final int max_connection = 5;

    // 分页大小，可自定义，取决于内存大小
    public static final int limit = 10000;

    // 节点列表，必须按hash有序存储
    public List<NodeInfo> nodeInfos = new ArrayList<>(); // 离散的哈希值，要求尽可能均匀

    public HashRing(InputStream inputStream, Charset charset) throws IOException {
        init_(inputStream, charset);
    }

    protected void init_(InputStream inputStream, Charset charset) throws IOException {
        String result = "";
        if (inputStream != null) {
            result = InputStreamUtil.toStr(inputStream, charset);
        }
        if (!result.isEmpty()) {
            // 进行解析
            nodeInfos = JSON.parseArray(result, NodeInfo.class);
            initPool();
        }
    }

    /**
     * 为所有当前节点初始化连接池
     */
    protected void initPool() {
        for (NodeInfo nodeInfo : nodeInfos) {
            if (nodeInfo.connectionInfo != null) {
                nodeInfo.connectionInfo.initConnectionPool(max_connection);
            }
        }
    }

    /**
     * 距离均分方法初始化多个节点
     *
     * @param number 节点数
     */
    public void init(int number, List<ConnectionInfo> connectionInfos) {
        nodeInfos.clear();
        long average_distance = max_hash / number;
        long hash = 0;
        for (int i = 0; i < number; i++) {
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.hash = hash;
            if (i != number - 1) {
                nodeInfo.distance = average_distance;
            } else {
                nodeInfo.distance = max_hash - hash + 1;
            }
            hash += average_distance;
            if (connectionInfos != null && connectionInfos.size() - 1 >= i) {
                nodeInfo.connectionInfo = connectionInfos.get(i);
            }
            nodeInfos.add(nodeInfo);
        }
        initPool(); // 自动初始化连接池
    }

    /**
     * 持久化节点配置
     *
     * @param outputStream 输出流
     * @throws IOException
     */
    public void save(OutputStream outputStream) throws IOException {
        System.out.println(nodeInfos);
        String str = JSONArray.toJSONString(nodeInfos, true);
        OutputStreamUtil.writeString(str, outputStream);
    }

    /**
     * 新增1个节点，自动选择最佳位置
     *
     * @param connectionInfo 节点信息，可为空
     * @return 发生迁移数据的节点
     */
    public int addNode(ConnectionInfo connectionInfo) throws SQLException {
        // 寻找最大距离节点
        int max_index = -1;
        long max_distance = -1;
        for (int i = 0; i < nodeInfos.size(); i++) {
            NodeInfo nodeInfo = nodeInfos.get(i);
            if (nodeInfo.distance > max_distance) {
                max_distance = nodeInfo.distance;
                max_index = i;
            }
        }
        NodeInfo selectNode = nodeInfos.get(max_index);
        long new_distance = max_distance / 2;
        // 选出的节点之后插入新节点
        NodeInfo newNodeInfo = new NodeInfo();
        newNodeInfo.hash = selectNode.hash + new_distance;
        newNodeInfo.connectionInfo = connectionInfo;
        nodeInfos.add(max_index + 1, newNodeInfo);
        if (max_index + 1 == nodeInfos.size() - 1) {
            newNodeInfo.distance = max_hash - newNodeInfo.distance + 1;
        } else {
            newNodeInfo.distance = nodeInfos.get(max_index + 2).hash - newNodeInfo.hash;
        }
        selectNode.distance = newNodeInfo.hash - selectNode.hash;
        newNodeInfo.connectionInfo.initConnectionPool(max_connection);
        // 进行数据迁移
        PoolConnection pc1 = selectNode.connectionInfo.acquireConnectionFromPool();
        PoolConnection pc2 = newNodeInfo.connectionInfo.acquireConnectionFromPool();
        Iterator<Data> dataIterator = DataMapper.selectAll(pc1.jdbcConnection, limit);
        while (dataIterator.hasNext()) {
            Data data = dataIterator.next();
            long index = locateNode(data.md5);
            if (index == max_index + 1) {
                DataMapper.saveData(data, pc2.jdbcConnection);
                DataMapper.deleteById(pc1.jdbcConnection, data.id);
            } else if (index != max_index) {
                throw new RuntimeException("can't locate the right node!");
            }
        }
        // JDBC连接归还池，而不是直接释放
        pc1.giveBack();
        pc2.giveBack();
        return max_index;
    }

    /**
     * 求文件总数
     *
     * @return
     * @throws SQLException
     */
    public long filesCount() throws SQLException {
        long c = 0;
        for (NodeInfo nodeInfo : nodeInfos) {
            if (nodeInfo.connectionInfo != null) {
                PoolConnection poolConnection = nodeInfo.connectionInfo.acquireConnectionFromPool();
                c += DataMapper.count(poolConnection.getJdbcConnection());
                poolConnection.giveBack();
            }
        }
        return c;
    }


    /**
     * 移除指定节点
     *
     * @param index 节点索引
     */
    public void remove(int index) throws SQLException {
        // 不允许移除0号节点，即至少有一个节点
        if (nodeInfos.size() <= 1 || index == 0) {
            throw new RuntimeException("fail to remove node: " + index);
        }
        NodeInfo srcNode = nodeInfos.get(index);
        NodeInfo dstNode = nodeInfos.get(index - 1);
        PoolConnection srcPC = srcNode.connectionInfo.acquireConnectionFromPool();
        PoolConnection dstPc = dstNode.connectionInfo.acquireConnectionFromPool();
        Iterator<Data> dataIterator = DataMapper.selectAll(srcPC.getJdbcConnection(), limit);
        while (dataIterator.hasNext()) {
            Data data = dataIterator.next();
            DataMapper.saveData(data, dstPc.getJdbcConnection());
        }
        srcPC.giveBack();
        dstPc.giveBack();
        srcNode = nodeInfos.remove(index); // 移除节点
        srcNode.connectionInfo.releasePool(); // 释放被移除节点的连接池
    }

    /**
     * 自动移除最佳节点
     *
     * @return 被移除节点号
     */
    public int remove() throws SQLException {
        if (nodeInfos.size() <= 1) {
            throw new RuntimeException("fail to remove node");
        }
        long min_distance = nodeInfos.get(0).distance;
        int min_index = 0;
        for (int i = 1; i < nodeInfos.size(); i++) {
            NodeInfo nodeInfo = nodeInfos.get(i);
            if (nodeInfo.distance < min_distance) {
                min_index = i;
                min_distance = nodeInfo.distance;
            }
        }
        remove(min_index);
        return min_index;
    }

    /**
     * 根据MD5查找节点，用于存取data
     *
     * @param md5 MD5
     * @return 节点编号
     * @throws SQLException
     */
    protected int locateNode(long md5) throws SQLException {
        // 二分查找
        int low = 0;
        int high = nodeInfos.size() - 1;
        while (high - low > 1) {
            int mid = (high + low) / 2;
            long midHash = nodeInfos.get(mid).hash;
            if (midHash > md5) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return low;
    }

    /**
     * 查询数据
     *
     * @param data data设置bucketName和fileName后就可以查询
     * @return
     * @throws SQLException
     */
    public Data getData(Data data) throws SQLException {
        long md5 = data.md5;
        PoolConnection connection = nodeInfos.get(locateNode(md5)).connectionInfo.acquireConnectionFromPool();
        Data d = DataMapper.queryById(data.id, connection.jdbcConnection);
        connection.giveBack(); // 归还连接
        return d;
    }

    /**
     * 存储数据
     *
     * @param data
     * @throws SQLException
     */
    public void saveData(Data data) throws SQLException {
        long md5 = data.md5;
        PoolConnection connection = nodeInfos.get(locateNode(md5)).connectionInfo.acquireConnectionFromPool();
        DataMapper.saveData(data, connection.jdbcConnection);
        connection.giveBack(); // 归还连接
    }

    /**
     * 删除数据
     *
     * @param data
     * @return 删除是否成功，如果数据不存在也会返回false
     * @throws SQLException
     */
    public boolean deleteData(Data data) throws SQLException {
        long md5 = data.md5;
        PoolConnection connection = nodeInfos.get(locateNode(md5)).connectionInfo.acquireConnectionFromPool();
        boolean result = DataMapper.deleteById(connection.getJdbcConnection(), data.id);
        connection.giveBack();
        return result;
    }

    // 仅摧毁连接池和内存存储，持久化的文件不会改变
    public void destroy() throws SQLException {
        for (NodeInfo nodeInfo : nodeInfos) {
            nodeInfo.connectionInfo.releasePool();
        }
        nodeInfos.clear();
    }
}
